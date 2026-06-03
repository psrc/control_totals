"""Shared MySQL credential and connection helpers.

Centralizes access to the UrbanSim parcel base-year MySQL databases used by the
control-totals steps (``load_split_hct_base_data``, ``regionalCTs``,
``split_ct_to_hct``). Credentials are resolved from environment variables whose
names are configured once in the ``urbansim_mysql`` settings block, with a
plain-text credentials file as a fallback.
"""
import os
from pathlib import Path
from urllib.parse import quote_plus

from sqlalchemy import create_engine


# Default environment variable names (overridable via the urbansim_mysql block).
DEFAULT_USER_ENV = 'URBANSIM_MYSQL_USER'
DEFAULT_PASSWORD_ENV = 'URBANSIM_MYSQL_PASSWORD'
DEFAULT_HOST_ENV = 'URBANSIM_MYSQL_HOST'


def read_mysql_creds(creds_path=None, user_env=DEFAULT_USER_ENV,
                     password_env=DEFAULT_PASSWORD_ENV, host_env=DEFAULT_HOST_ENV):
    """Resolve MySQL credentials from environment variables or a credentials file.

    Prefers environment variables (whose names default to ``URBANSIM_MYSQL_USER``,
    ``URBANSIM_MYSQL_PASSWORD``, and ``URBANSIM_MYSQL_HOST`` but can be overridden
    via settings). Falls back to a plain-text credentials file (three non-empty
    lines: username, password, host) when one or more env vars are missing and
    ``creds_path`` exists.

    Args:
        creds_path (pathlib.Path, optional): Path to a fallback credentials file.
        user_env (str, optional): Env var name for the MySQL username.
        password_env (str, optional): Env var name for the MySQL password.
        host_env (str, optional): Env var name for the MySQL host.

    Returns:
        tuple[str, str, str]: ``(username, password, host)``.

    Raises:
        ValueError: If credentials cannot be resolved from env vars or file.
    """
    user = os.environ.get(user_env)
    password = os.environ.get(password_env)
    host = os.environ.get(host_env)
    if user and password and host:
        return user, password, host

    if creds_path is not None and Path(creds_path).exists():
        lines = [line.strip() for line in Path(creds_path).read_text().splitlines() if line.strip()]
        if len(lines) < 3:
            raise ValueError('Expected username, password, and host in creds file')
        return lines[0], lines[1], lines[2]

    raise ValueError(
        f'MySQL credentials not found. Set {user_env}, {password_env}, '
        f'and {host_env} environment variables, or provide a creds file.'
    )


def get_mysql_engine(database, creds_path=None, user_env=DEFAULT_USER_ENV,
                     password_env=DEFAULT_PASSWORD_ENV, host_env=DEFAULT_HOST_ENV):
    """Create a SQLAlchemy engine for a MySQL database.

    Resolves credentials via :func:`read_mysql_creds` and builds a
    ``mysql+pymysql`` connection string with URL-escaped credentials.

    Args:
        database (str): Name of the MySQL database (e.g.
            ``'2023_parcel_baseyear'``).
        creds_path (pathlib.Path, optional): Path to a fallback credentials file.
        user_env (str, optional): Env var name for the MySQL username.
        password_env (str, optional): Env var name for the MySQL password.
        host_env (str, optional): Env var name for the MySQL host.

    Returns:
        sqlalchemy.engine.Engine: Engine connected to ``database``.
    """
    user, password, host = read_mysql_creds(
        creds_path, user_env=user_env, password_env=password_env, host_env=host_env
    )
    return create_engine(
        f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@{host}/{database}"
    )


def get_mysql_config(pipeline):
    """Read the centralized ``urbansim_mysql`` settings block.

    Provides a single source of truth for the parcel base-year database, the
    credentials file, and the names of the environment variables holding the
    MySQL username/password/host. All keys are optional and fall back to the
    historical defaults.

    Args:
        pipeline (Pipeline): The data pipeline (used for settings and the data
            directory).

    Returns:
        dict: Keys ``parcel_base_year`` (int), ``database`` (str),
            ``creds_path`` (pathlib.Path), ``user_env`` (str),
            ``password_env`` (str), ``host_env`` (str).
    """
    cfg = pipeline.settings.get('urbansim_mysql', {}) or {}
    base_year = int(pipeline.settings.get('base_year', 0) or 0)
    parcel_base_year = int(cfg.get('parcel_base_year', base_year))

    data_dir = Path(pipeline.get_data_dir())
    creds_path = data_dir / cfg.get('creds_file', 'creds.txt')

    return {
        'parcel_base_year': parcel_base_year,
        'database': cfg.get('database', f'{parcel_base_year}_parcel_baseyear'),
        'creds_path': creds_path,
        'user_env': cfg.get('user_env', DEFAULT_USER_ENV),
        'password_env': cfg.get('password_env', DEFAULT_PASSWORD_ENV),
        'host_env': cfg.get('host_env', DEFAULT_HOST_ENV),
    }
