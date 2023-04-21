CREATE ROLE adm;
GRANT ALL PRIVILEGES ON DATABASE postgres TO adm; -- postgres название базы данных
ALTER ROLE adm WITH SUPERUSER;

CREATE ROLE guest WITH LOGIN PASSWORD '222';
GRANT CONNECT ON DATABASE postgres TO guest;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO guest;

-- удаление ролей
REASSIGN OWNED BY adm TO postgres;
DROP OWNED BY adm;
DROP ROLE adm;

REASSIGN OWNED BY guest TO postgres;
DROP OWNED BY guest;
drop role guest;