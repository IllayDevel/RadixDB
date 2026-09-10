import sql from 'shiki/langs/sql.mjs';

const [baseSql] = sql;

export const radixDbDataTypes = [
  'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT',
  'FLOAT', 'DOUBLE', 'REAL', 'DECIMAL', 'NUMERIC',
  'TEXT', 'VARCHAR', 'CHAR', 'STRING', 'CLOB',
  'BOOLEAN', 'BOOL',
  'TIMESTAMP', 'TIMESTAMPTZ', 'DATETIME', 'TIME', 'DATE',
  'JSON', 'JSONB', 'UUID', 'BYTES', 'BLOB', 'BINARY', 'VARBINARY',
  'VECTOR', 'ARRAY', 'ROWTYPE',
];

// Keep this list aligned with the public SQL lexer in RadixDB 1.2.
export const radixDbKeywords = [
  'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
  'DELETE', 'CREATE', 'REPLACE', 'TABLE', 'DROP', 'ALTER', 'ADD', 'COLUMN',
  'AND', 'OR', 'XOR', 'NOT', 'NULL', 'PRIMARY', 'PRAGMA', 'KEY',
  'AUTO_INCREMENT', 'AUTOINCREMENT', 'DEFAULT', 'AS', 'OF', 'DISTINCT',
  'ORDER', 'BY', 'ASC', 'DESC', 'LIMIT', 'OFFSET', 'GROUP', 'HAVING',
  'JOIN', 'INNER', 'OUTER', 'LEFT', 'RIGHT', 'FULL', 'ON', 'DUPLICATE',
  'CONFLICT', 'DO', 'NOTHING', 'USING', 'CROSS', 'NATURAL', 'TRUE', 'FALSE',
  'CASE', 'CAST', 'EXTRACT', 'WHEN', 'THEN', 'ELSE', 'END', 'BETWEEN', 'IN',
  'IS', 'LIKE', 'ILIKE', 'ESCAPE', 'GLOB', 'REGEXP', 'RLIKE', 'EXISTS',
  'ALL', 'ANY', 'SOME', 'IF', 'UNION', 'INTERSECT', 'EXCEPT', 'WITH',
  'UNIQUE', 'CHECK', 'CONSTRAINT', 'FOREIGN', 'REFERENCES', 'SHOW',
  'DESCRIBE', 'TABLES', 'VIEWS', 'INDEXES', 'CASCADE', 'RESTRICT', 'INDEX',
  'VIEW', 'TRIGGER', 'PROCEDURE', 'FUNCTION', 'RETURNING', 'OVER',
  'PARTITION', 'RANGE', 'ROWS', 'WINDOW', 'UNBOUNDED', 'BEGIN',
  'TRANSACTION', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE', 'PRECEDING',
  'FOLLOWING', 'CURRENT', 'ROW', 'MODIFY', 'RENAME', 'TO', 'ISOLATION',
  'LEVEL', 'READ', 'COMMITTED', 'UNCOMMITTED', 'INTERVAL', 'RECURSIVE',
  'NULLS', 'FIRST', 'LAST', 'TRUNCATE', 'FILTER', 'EXPLAIN', 'ANALYZE',
  'FETCH', 'NEXT', 'ONLY', 'VACUUM', 'COPY', 'FORMAT', 'HEADER', 'DELIMITER',
  'DECLARE', 'CURSOR', 'CONSTANT', 'ELSIF', 'LOOP', 'WHILE', 'FOR',
  'REVERSE', 'EXIT', 'CONTINUE', 'RETURN', 'QUERY', 'EXCEPTION', 'RAISE',
  'OTHERS', 'OPEN', 'CLOSE', 'CALL', 'PERFORM', 'EXECUTE', 'RETURNS',
  'LANGUAGE', 'RADIX', 'OUT', 'INOUT', 'IMMUTABLE', 'STABLE', 'VOLATILE',
  'SECURITY', 'INVOKER', 'DEFINER', 'SEARCH', 'PATH', 'RESOURCE', 'POLICY',
  'STRICT', 'PRIORITY', 'BEFORE', 'AFTER', 'EACH', 'STATEMENT', 'OLD', 'NEW',
  'JOB', 'SCHEDULE', 'EVERY', 'AT', 'ENABLE', 'DISABLE', 'RUN', 'PRINCIPAL',
  'ROLE', 'SCHEMA', 'DATABASE', 'GRANT', 'REVOKE', 'CONNECT', 'USAGE',
  'OWNER', 'ADMIN', 'OPTION', 'FOUND', 'NOTFOUND', 'ROWCOUNT', 'ISOPEN',
  'EXTENSION', 'TYPE', 'VERSION', 'NATIVE', 'OPERATOR', 'CLASS', 'PLANNER',
  'SUPPORT', 'LEFTARG', 'RIGHTARG', 'PASSWORD',
];

const wordPattern = (words) => `(?i:\\b(?:${words.join('|')})\\b)`;

export const radixDbSql = {
  ...baseSql,
  name: 'radixdb-sql',
  displayName: 'RadixDB SQL',
  scopeName: 'source.radixdb-sql',
  patterns: [
    { match: wordPattern(radixDbDataTypes), name: 'storage.type.radixdb.sql' },
    ...baseSql.patterns,
    { match: wordPattern(radixDbKeywords), name: 'keyword.other.radixdb.sql' },
  ],
};

export default radixDbSql;
