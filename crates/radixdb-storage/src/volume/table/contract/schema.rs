macro_rules! segmented_table_schema_methods {
    () => {
        // =========================================================================
        // Metadata
        // =========================================================================

        fn name(&self) -> &str {
            self.hot.name()
        }

        fn schema(&self) -> &Schema {
            self.hot.schema()
        }

        fn txn_id(&self) -> i64 {
            self.hot.txn_id()
        }

        // =========================================================================
        // DDL — delegate to hot buffer
        // =========================================================================

        fn create_column(
            &mut self,
            name: &str,
            column_type: DataType,
            nullable: bool,
        ) -> Result<()> {
            self.hot.create_column(name, column_type, nullable)
        }

        fn create_column_with_default(
            &mut self,
            name: &str,
            column_type: DataType,
            nullable: bool,
            default_expr: Option<String>,
        ) -> Result<()> {
            self.hot
                .create_column_with_default(name, column_type, nullable, default_expr)
        }

        fn create_column_with_default_value(
            &mut self,
            name: &str,
            column_type: DataType,
            nullable: bool,
            default_expr: Option<String>,
            default_value: Option<Value>,
        ) -> Result<()> {
            self.hot.create_column_with_default_value(
                name,
                column_type,
                nullable,
                default_expr,
                default_value,
            )
        }

        fn drop_column(&mut self, name: &str) -> Result<()> {
            self.hot.drop_column(name)
        }

        fn materialize_insert_values(&mut self, row: &mut Row) -> Result<()> {
            self.hot.materialize_insert_values(row)
        }
    };
}

pub(super) use segmented_table_schema_methods;
