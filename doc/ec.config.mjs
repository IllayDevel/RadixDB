import { defineEcConfig } from '@astrojs/starlight/expressive-code';
import radixDbSql from './src/syntax/radixdb-sql.mjs';

export default defineEcConfig({
  shiki: {
    langs: [radixDbSql],
  },
});
