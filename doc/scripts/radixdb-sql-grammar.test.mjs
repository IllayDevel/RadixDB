import assert from 'node:assert/strict';
import test from 'node:test';
import { createHighlighter } from 'shiki';
import radixDbSql, {
  radixDbDataTypes,
  radixDbKeywords,
} from '../src/syntax/radixdb-sql.mjs';

const theme = 'github-dark';

async function tokensFor(code) {
  const highlighter = await createHighlighter({
    themes: [theme],
    langs: [radixDbSql],
  });
  try {
    return highlighter.codeToTokensBase(code, { lang: 'radixdb-sql', theme });
  } finally {
    highlighter.dispose();
  }
}

test('RadixDB SQL keywords receive syntax highlighting', async () => {
  const words = [...new Set([...radixDbKeywords, ...radixDbDataTypes])];
  const tokens = await tokensFor(`${words.join(';\n')};\nplain_identifier;`);
  const plainColor = tokens.at(-1)[0].color;

  for (const [index, word] of words.entries()) {
    const token = tokens[index].find(candidate => candidate.content === word);
    assert.ok(token, `${word} must have its own syntax token`);
    assert.notEqual(token.color, plainColor, `${word} must not use the plain text color`);
  }
});

test('RadixDB keywords inside strings and comments keep their original scopes', async () => {
  const tokens = await tokensFor("'DESCRIBE';\n-- PRAGMA\nDESCRIBE assets;");
  const stringColor = tokens[0][0].color;
  const commentColor = tokens[1][0].color;
  const keywordColor = tokens[2][0].color;

  assert.notEqual(stringColor, keywordColor);
  assert.notEqual(commentColor, keywordColor);
  assert.match(tokens[0][0].content, /DESCRIBE/);
  assert.match(tokens[1][0].content, /PRAGMA/);
});
