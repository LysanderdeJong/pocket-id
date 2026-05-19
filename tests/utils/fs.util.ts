import path from 'path';
import { fileURLToPath } from 'url';

export const tmpDir = pathFromRoot('.tmp');

export function pathFromRoot(p: string): string {
	return path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', p);
}
