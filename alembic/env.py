import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context
from pyinaturalist_convert._models import Base

config = context.config

# Set up logging from the alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override database URL with INAT_DB_PATH env var if set
db_path = os.environ.get('INAT_DB_PATH')
if db_path:
    config.set_main_option('sqlalchemy.url', f'sqlite:///{db_path}')

# Our model metadata for autogenerate support
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (without a live DB connection).

    Generates SQL script output rather than executing against a database.
    """
    url = config.get_main_option('sqlalchemy.url')
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={'paramstyle': 'named'},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (with a live DB connection)."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix='sqlalchemy.',
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
