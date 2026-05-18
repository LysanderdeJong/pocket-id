package utils_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pocket-id/pocket-id/backend/internal/common"
	"github.com/pocket-id/pocket-id/backend/internal/utils"
	testutils "github.com/pocket-id/pocket-id/backend/internal/utils/testing"
)

func TestCheckDatabaseVersionSupported(t *testing.T) {
	oldConfig := common.EnvConfig
	common.EnvConfig.DbProvider = common.DbProviderSqlite
	common.EnvConfig.AllowDowngrade = false
	t.Cleanup(func() { common.EnvConfig = oldConfig })

	db := testutils.NewDatabaseForTest(t)
	sqlDb, err := db.DB()
	require.NoError(t, err)

	info, err := utils.CheckDatabaseVersionSupported(t.Context(), sqlDb)
	require.NoError(t, err)
	require.False(t, info.Dirty)
	require.Equal(t, info.RequiredVersion, info.CurrentVersion)

	_, err = sqlDb.ExecContext(t.Context(), "UPDATE schema_migrations SET version = ?, dirty = ?", info.RequiredVersion+1, false)
	require.NoError(t, err)

	_, err = utils.CheckDatabaseVersionSupported(t.Context(), sqlDb)
	require.Error(t, err)
	require.True(t, errors.Is(err, utils.ErrDatabaseVersionTooNew))

	common.EnvConfig.AllowDowngrade = true
	_, err = utils.CheckDatabaseVersionSupported(t.Context(), sqlDb)
	require.NoError(t, err)

	common.EnvConfig.AllowDowngrade = false
	_, err = sqlDb.ExecContext(t.Context(), "UPDATE schema_migrations SET version = ?, dirty = ?", info.RequiredVersion, true)
	require.NoError(t, err)

	_, err = utils.CheckDatabaseVersionSupported(t.Context(), sqlDb)
	require.Error(t, err)
	require.True(t, errors.Is(err, utils.ErrDatabaseMigrationDirty))
}
