mod support;

#[allow(dead_code)]
mod generated_schema {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated_schema.rs"));
}

use generated_schema::{
    RtBranches, RtCustomerGroups, RtCustomers, RtProducts, RtSalesOrderLines, RtSalesOrders,
};
use radixdb_client::ExecuteResult;
use radixdb_orm::{Expr, FieldValue, Grouping, OrmBuilder};

const CUSTOMER_ID: i64 = 9_001;
const ORDER_ID: i64 = 9_001;
const LINE_ID: i64 = 9_001;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut connection = support::connect()?;

    // Raw SQL and generated ORM deliberately share this exact connection and
    // transaction. Cleanup makes the tutorial deterministic and repeatable.
    connection.begin()?;
    connection.execute(&format!(
        "DELETE FROM rt_sales_order_lines WHERE id = {LINE_ID}"
    ))?;
    connection.execute(&format!(
        "DELETE FROM rt_sales_orders WHERE id = {ORDER_ID}"
    ))?;
    connection.execute(&format!(
        "DELETE FROM rt_customers WHERE id = {CUSTOMER_ID}"
    ))?;

    let mut customer = RtCustomers::new();
    customer.id.set(CUSTOMER_ID);
    customer.group_id.set(RtCustomerGroups::reference(1));
    customer.customer_no.set("CUS-ORM-9001".to_string());
    customer.name.set("ORM Trading Customer".to_string());
    customer.email.set("orm.customer@example.test".to_string());
    customer.city.set("Barnaul".to_string());
    customer.insert(&mut connection)?;

    let mut order = RtSalesOrders::new();
    order.id.set(ORDER_ID);
    order.customer_id.set(RtCustomers::reference(CUSTOMER_ID));
    order.branch_id.set(RtBranches::reference(1));
    order.order_no.set("SO-ORM-9001".to_string());
    order.order_date.set("2026-08-22".to_string());
    order.status.set("new".to_string());
    order.total_cents.set(58_000);
    order.paid_cents.set(0);
    order.insert(&mut connection)?;

    let mut line = RtSalesOrderLines::new();
    line.id.set(LINE_ID);
    line.sales_order_id.set(RtSalesOrders::reference(ORDER_ID));
    line.product_id.set(RtProducts::reference(1));
    line.quantity.set(2);
    line.price_cents.set(29_000);
    line.discount_cents.set(0);
    line.insert(&mut connection)?;

    let mut loaded = RtSalesOrders::get(ORDER_ID).one(&mut connection)?;
    if let FieldValue::Value { value } = loaded.status.value() {
        println!("loaded order status: {value}");
    }
    loaded.status.set("confirmed".to_string());
    loaded.save(&mut connection)?;

    let customer_name = RtSalesOrderLines::SALES_ORDER_ID
        .field(RtSalesOrders::CUSTOMER_ID)
        .field(RtCustomers::NAME);
    let product_name = RtSalesOrderLines::PRODUCT_ID.field(RtProducts::NAME);

    let navigation = RtSalesOrderLines::query()
        .select_projections(vec![
            RtSalesOrderLines::ID.projection(),
            RtSalesOrderLines::SALES_ORDER_ID
                .field(RtSalesOrders::ORDER_NO)
                .alias("order_no"),
            customer_name.clone().alias("customer_name"),
            product_name.clone().alias("product_name"),
            RtSalesOrderLines::QUANTITY.projection(),
            RtSalesOrderLines::PRICE_CENTS.projection(),
        ])
        .filter(RtSalesOrderLines::SALES_ORDER_ID.eq(RtSalesOrders::reference(ORDER_ID)));

    println!("\nnavigation IR:\n{}", navigation.to_json()?);
    println!("navigation SQL: {}", navigation.to_sql()?.sql);
    let ExecuteResult::Cursor(cursor) = navigation.fetch(&mut connection)? else {
        return Err("navigation SELECT did not return a cursor".into());
    };
    support::print_cursor(&mut connection, &cursor)?;

    let revenue = Expr::aggregate(
        "SUM",
        [RtSalesOrderLines::PRICE_CENTS.mul(RtSalesOrderLines::QUANTITY)],
        false,
        None,
        vec![],
    );
    let aggregate = RtSalesOrderLines::query()
        .select_projections(vec![
            product_name.clone().alias("product_name"),
            revenue.alias("gross_cents"),
        ])
        .filter(RtSalesOrderLines::SALES_ORDER_ID.eq(RtSalesOrders::reference(ORDER_ID)))
        .group_by(Grouping::Expressions {
            expressions: vec![product_name.expr().0],
        });

    println!("\naggregate SQL: {}", aggregate.to_sql()?.sql);
    let ExecuteResult::Cursor(cursor) = aggregate.fetch(&mut connection)? else {
        return Err("aggregate SELECT did not return a cursor".into());
    };
    support::print_cursor(&mut connection, &cursor)?;

    // A direct query remains available without a second connection or ORM
    // wrapper. It observes the same uncommitted transaction state.
    let ExecuteResult::Cursor(cursor) = connection.execute(&format!(
        "SELECT status, total_cents FROM rt_sales_orders WHERE id = {ORDER_ID}"
    ))?
    else {
        return Err("raw SELECT did not return a cursor".into());
    };
    println!("\nraw SQL on the same connection:");
    support::print_cursor(&mut connection, &cursor)?;

    connection.commit()?;
    connection.shutdown()?;
    Ok(())
}
