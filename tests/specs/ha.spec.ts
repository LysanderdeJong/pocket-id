import { expect, request as playwrightRequest, test, type APIRequestContext } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { oidcClients, refreshTokens } from '../data';

const nodeURLs = [process.env.HA_NODE_1_URL, process.env.HA_NODE_2_URL, process.env.HA_NODE_3_URL].filter(
	(url): url is string => !!url
);

const containerByURL = new Map<string, string>([
	[process.env.HA_NODE_1_URL ?? '', process.env.HA_NODE_1_CONTAINER ?? 'pocket-id-ha-node1'],
	[process.env.HA_NODE_2_URL ?? '', process.env.HA_NODE_2_CONTAINER ?? 'pocket-id-ha-node2'],
	[process.env.HA_NODE_3_URL ?? '', process.env.HA_NODE_3_CONTAINER ?? 'pocket-id-ha-node3']
]);

test.describe('HA multi-replica behavior', () => {
	test.skip(nodeURLs.length !== 3, 'Set HA_NODE_1_URL, HA_NODE_2_URL and HA_NODE_3_URL to run HA tests');
	test.describe.configure({ mode: 'serial' });

	let nodes: APIRequestContext[];

	test.beforeAll(async () => {
		nodes = await Promise.all(
			nodeURLs.map((baseURL) => playwrightRequest.newContext({ baseURL }))
		);

		const reset = await nodes[0].post('/api/test/reset?skip-ldap=true');
		expect(reset.status()).toBe(204);
	});

	test.afterAll(async () => {
		await Promise.all(nodes?.map((node) => node.dispose()) ?? []);
	});

	test('OIDC refresh token flow works across replicas', async () => {
		const refreshToken = refreshTokens.find((token) => !token.expired)!;

		const signedRefreshToken = await nodes[1]
			.post('/api/test/refreshtoken', {
				data: {
					rt: refreshToken.token,
					client: refreshToken.clientId,
					user: refreshToken.userId
				}
			})
			.then((response) => response.text());

		const tokenResponse = await nodes[2].post('/api/oidc/token', {
			headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
			form: {
				grant_type: 'refresh_token',
				client_id: refreshToken.clientId,
				refresh_token: signedRefreshToken,
				client_secret: oidcClients.nextcloud.secret
			}
		});

		expect(tokenResponse.status()).toBe(200);
		const tokenData = await tokenResponse.json();
		expect(tokenData.access_token).toBeDefined();
		expect(tokenData.refresh_token).toBeDefined();
		expect(tokenData.id_token).toBeDefined();
		expect(tokenData.token_type.toLowerCase()).toBe('bearer');
	});

	test('app config changes reload across replicas', async () => {
		test.setTimeout(60_000);

		const value = `Pocket ID HA ${Date.now()}`;
		const update = await nodes[0].post('/api/test/app-config/app-name', { data: { value } });
		expect(update.status()).toBe(204);

		await expect
			.poll(async () => {
				const response = await nodes[1].get('/api/test/app-config/app-name');
				return (await response.json()).value;
			}, { timeout: 45_000, intervals: [1_000] })
			.toBe(value);
	});

	test('scheduler jobs queued on standby replay after failover', async () => {
		test.setTimeout(90_000);

		const leadership = await Promise.all(
			nodes.map(async (node, index) => ({
				index,
				url: nodeURLs[index],
				...(await node.get('/api/test/leadership').then((response) => response.json()))
			}))
		);
		const leader = leadership.find((node) => node.leader);
		const standbys = leadership.filter((node) => !node.leader);
		expect(leader).toBeDefined();
		expect(standbys.length).toBeGreaterThan(0);

		const probeName = `probe-${Date.now()}`;
		for (const standby of standbys) {
			const register = await nodes[standby.index].post(`/api/test/scheduler-probe/${probeName}`);
			expect(register.status()).toBe(204);
		}

		await expect
			.poll(async () => {
				for (const standby of standbys) {
					const response = await nodes[standby.index].get(`/api/test/scheduler-probe/${probeName}`);
					const body = await response.json();
					if (body.value) return body.value;
				}
				return '';
			}, { timeout: 2_000, intervals: [500] })
			.toBe('');

		const leaderContainer = containerByURL.get(leader!.url);
		expect(leaderContainer).toBeDefined();
		execFileSync('docker', ['stop', leaderContainer!], { stdio: 'inherit' });

		await expect
			.poll(async () => {
				for (const [index, node] of nodes.entries()) {
					if (index === leader!.index) continue;
					const response = await node.get(`/api/test/scheduler-probe/${probeName}`);
					const body = await response.json();
					if (body.value) return body.value;
				}
				return '';
			}, { timeout: 60_000, intervals: [1_000] })
			.not.toBe('');
	});
});
