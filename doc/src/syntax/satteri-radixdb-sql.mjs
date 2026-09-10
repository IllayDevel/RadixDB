export default {
  name: 'radixdb-sql-language',
  code(node, context) {
    if (node.lang === 'sql') context.setProperty(node, 'lang', 'radixdb-sql');
  },
};
