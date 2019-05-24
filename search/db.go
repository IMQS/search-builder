package search

import (
	"database/sql"

	//"fmt"
	"strings"

	"github.com/BurntSushi/migration"
	"github.com/lib/pq"
)

// At some point we're going to need to make this configurable
const ROWID = "rowid"

func OpenIndexDB(cfg, genericCfg *ConfigDatabase) (*sql.DB, error) {
	migrations := createMigrations(genericCfg)
	db, err := migration.Open(cfg.Driver, cfg.DSN(), migrations)
	if err == nil {
		setDBConnectionLimits(cfg, db)
		return db, err
	}
	// Got the following error on 'prefix', when 'searchindex' DB did not exist:
	// Error initializing search engine: Error connecting to search index database: Could not get DB version: WSARecv tcp 127.0.0.1:60064: An existing connection was forcibly closed by the remote host.
	if strings.Index(err.Error(), "pq: database \""+cfg.Database+"\" does not exist") != -1 {
		eCreate := createDB(cfg)
		if eCreate != nil {
			return nil, err
		}
		db, err = migration.Open(cfg.Driver, cfg.DSN(), migrations)
	}
	if db != nil {
		setDBConnectionLimits(cfg, db)
	}
	return db, err
}

func setDBConnectionLimits(cfg *ConfigDatabase, db *sql.DB) {
	if cfg.MaxIdleConns != 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.MaxOpenConns != 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
}

func createDB(cfg *ConfigDatabase) error {
	// Connect via the Postgres database, so that we can create the DB
	cfg2 := *cfg
	cfg2.Database = "postgres"
	db, err := sql.Open(cfg.Driver, cfg2.DSN())
	if err != nil {
		return err
	}
	defer db.Close()
	_, eExec := db.Exec("CREATE DATABASE \"" + cfg.Database + "\"")
	return eExec
}

func escapeSqlIdent(db *sql.DB, ident string) string {
	return "\"" + ident + "\""
}

func escapeSqlLiteral(db *sql.DB, literal string) string {
	return "'" + strings.Replace(literal, "'", "''", -1) + "'"
}

func createMigrations(genericCfg *ConfigDatabase) []migration.Migrator {
	var migrations []migration.Migrator

	// Each of these migrations corresponds to a version in whole migration.
	// If a migration has more than one SQL statement, you must separate them with semicolons.

	/*
		--- Migration 1 ---
		We have two options here:
		1. No primary key on searchindex.
		   Keep an index on token
		   Keep an index on srctab
		2. Primary key is (token, srctab, srcrow)
		   Keep an index on srctab

		Statistics, with 1 million records, as generated by the script inside benchmark.go
		Times are a single sample, after 3 runs.
		1. Table Size 52 MB. Indexes Size 120 MB. Query 0.235 ms. Populate swift (420k records) 9.5 seconds
		2. Table Size 52 MB. Indexes Size 124 MB. Query 0.235 ms. Populate swift (420k records) 9.1 seconds

		The numbers are pretty equal.
		I'm going with option (2) since it helps us make sure we don't screw up, by enforcing key uniqueness.

		This is the schema for option (1)
		CREATE TABLE searchindex(token VARCHAR, srctab INTEGER, srcrow BIGINT);
		CREATE INDEX idx_searchindex_token on searchindex(token);
		CREATE INDEX idx_searchindex_srctab on searchindex(srctab);
	*/
	migrations = append(migrations, makeMigrationFromSQL(
		`CREATE TABLE searchindex(token VARCHAR, srctab INTEGER, srcrow BIGINT, PRIMARY KEY(token, srctab, srcrow));
		CREATE INDEX idx_searchindex_srctab ON searchindex(srctab);
		CREATE TABLE searchtables(tablename VARCHAR, srctab INTEGER, PRIMARY KEY(tablename));
		CREATE INDEX idx_searchtables_srctab ON searchtables(srctab);`))

	/*
		--- Migration 2 ---
		Added modstamp
	*/
	migrations = append(migrations, makeMigrationFromSQL(
		`DELETE FROM searchindex;
		DELETE FROM searchtables;
		ALTER TABLE searchtables ADD COLUMN modstamp VARCHAR;`))

	/*
		--- Migration 3 ---
		Why COLLATION "C"? Without that, we can't generate ranges reliably. For example, '9' + 1 = ':',
		but the default Postgres collation doesn't agree with that. By using the "C" collation, we get
		to treat strings as simply binary byte strings.
	*/
	migrations = append(migrations, makeMigrationFromSQL(
		`DELETE FROM searchtables;
		DROP TABLE searchindex;
		CREATE TABLE searchindex(token VARCHAR COLLATE "C", srctab INTEGER, srcrow BIGINT, PRIMARY KEY(token, srctab, srcrow));
		CREATE INDEX idx_searchindex_srctab ON searchindex(srctab);`))

	/*
		--- Migration 4 ---
		Automatic vacuums are taking down some old servers with HDDs. SSD servers are fine.
		See 'doc.go' for more.
	*/
	migrations = append(migrations, makeMigrationFromSQL(
		`ALTER TABLE searchindex SET (autovacuum_enabled = false);`))

	/*
		--- Migration 5 ---
		Fixup: srctab must be unique
	*/
	migrations = append(migrations, makeMigrationFromSQL(
		`DROP INDEX idx_searchtables_srctab;
		CREATE UNIQUE INDEX idx_searchtables_srctab ON searchtables(srctab);`))

	/*
		--- Migration 6 ---
		Upgrade the 'srctab' concept to the 'srcfield' concept. A 'srcfield'
		is (srctab << 16 | srcfield). It allows us to limit our search results by field.
		Introduced the concept of searchnametable, which is a generic lookup table from
		string to small integer. We use this for both table names and field names, so that
		we can form a unique integer for every field (ie srctab << 16 | srcfield mentioned above).
		Change naming convention to use underscores.
	*/
	migrations = append(migrations, makeMigrationFromSQL(
		`DROP TABLE searchindex;
		DROP TABLE searchtables;
		CREATE TABLE search_index(token VARCHAR COLLATE "C", srcfield INTEGER, srcrow BIGINT, PRIMARY KEY(token, srcfield, srcrow));
		CREATE INDEX idx_search_index_srcfield ON search_index(srcfield);
		ALTER TABLE search_index SET (autovacuum_enabled = false);
		CREATE TABLE search_nametable(name VARCHAR, id INTEGER, PRIMARY KEY(name));
		CREATE UNIQUE INDEX idx_search_nametable_id ON search_nametable(id);
		CREATE TABLE search_src_tables(tablename VARCHAR, modstamp VARCHAR, PRIMARY KEY(tablename));`))

	/*
		--- Migration 7 ---
		Change (srcrow BIGINT) to (srcrows BYTEA), so that we can represent relationships tables.
	*/
	migrations = append(migrations, makeMigrationFromSQL(
		`DROP TABLE search_index;
		CREATE TABLE search_index(token VARCHAR COLLATE "C", srcfield INTEGER, srcrows BYTEA, PRIMARY KEY(token, srcfield, srcrows));
		CREATE INDEX idx_search_index_srcfield ON search_index(srcfield);
		ALTER TABLE search_index SET (autovacuum_enabled = false);`))

	/*
		--- Migration 8 ---
		Create new table that will store additional config settings to the file-based config.
		We can now store configuration settings inside the database (in addition to the JSON config file)
	*/
	migrations = append(migrations, makeMigrationFromSQL(
		`CREATE TABLE search_config (dbname VARCHAR, tablename VARCHAR, config JSONB, PRIMARY KEY (dbname, tablename));`))

	/*
		--- Migration 9 ---
		In the generic system, every time there is a new import,
		a new table name is reserved for each of the files contained in the import.
		That name which will look like, for example g_table_2, where the last digit is incremented for each new table,
		will change with each import of the same file. So to keep track of all these changes we created another
		field called the tablename_external in the IMQS_METATABLE on the generic database which keeps tracks of the import file name.

		The issue then becomes what should happen to the already indexed fields for that generic table which are being
		stored in the search_config table, when the generic file is reimported or deleted from the original database?
		Thats why we have added a new field to the search_config table called the long_lived_name. Just like
		the table_external_name in the IMQS MetaTable table, this field will store the original name of the table
		and we expect it to never change between diffent re-imports.

		We can update or delete any search_config using long_lived_name field as a reference
	*/
	migrations = append(migrations, func(tx migration.LimitedTx) error {
		tableNames := make([]string, 0)
		nameRows, err := tx.Query(`SELECT tablename FROM search_config`)
		if err != nil {
			return err
		}
		defer nameRows.Close()
		for nameRows.Next() {
			var name string
			if err := nameRows.Scan(&name); err != nil {
				return err
			}
			tableNames = append(tableNames, name)
		}

		migrationSQL := `ALTER TABLE search_config DROP CONSTRAINT search_config_pkey; ALTER TABLE search_config ADD COLUMN longlived_name VARCHAR;`
		if len(tableNames) > 0 {
			genericDB, err := sql.Open(genericCfg.Driver, genericCfg.DSN())
			if err != nil {
				return err
			}
			defer genericDB.Close()

			metaRows, err := genericDB.Query(`SELECT "TableNameInternal", "TableNameExternal" FROM "ImqsMetaTable" WHERE "TableNameInternal" = any($1)`, pq.Array(tableNames))
			if err != nil {
				return err
			}
			defer metaRows.Close()

			for metaRows.Next() {
				var internalName, externalName string
				if err := metaRows.Scan(&internalName, &externalName); err != nil {
					return err
				}
				migrationSQL += `UPDATE search_config SET longlived_name = '` + externalName + `' WHERE tablename = '` + internalName + `'; `
			}
		}
		migrationSQL += `ALTER TABLE search_config ADD CONSTRAINT search_config_pkey PRIMARY KEY (dbname, longlived_name);`

		_, err = tx.Exec(migrationSQL)
		return err
	})

	return migrations
}

func makeMigrationFromSQL(sql string) migration.Migrator {
	return func(tx migration.LimitedTx) error {
		_, err := tx.Exec(sql)
		return err
	}
}
