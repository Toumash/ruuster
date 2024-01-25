

use exchanges::ExchangeKind;

use crate::tests_utils::*;

use super::Empty;

#[tokio::test]
async fn test_declare_and_list_queues() {
    let mut client = setup_server_and_client().await;

    create_queues(
        &mut client,
        &["q1".to_string(), "q2".to_string(), "q3".to_string()],
        false,
    )
    .await;

    // adding queue with duplicate name should fail
    create_queues(
        &mut client,
        &["q1".to_string(), "q2".to_string(), "q3".to_string()],
        true,
    )
    .await;

    let response = client.list_queues(Empty {}).await;
    assert!(
        response.is_ok(),
        "listing queues failed: {}",
        response.unwrap_err()
    );

    let list = response.unwrap();
    assert_eq!(list.get_ref().queue_names.len(), 3);
}

#[tokio::test]
async fn test_declare_and_list_exchanges() {
    let mut client = setup_server_and_client().await;

    create_exchanges(
        &mut client,
        &["e1".to_string(), "e2".to_string(), "e3".to_string()],
        ExchangeKind::Fanout,
        false,
    )
    .await;

    create_exchanges(
        &mut client,
        &["e1".to_string(), "e2".to_string(), "e3".to_string()],
        ExchangeKind::Fanout,
        true,
    )
    .await;

    let response = client.list_exchanges(Empty {}).await;
    assert!(
        response.is_ok(),
        "failed to call list_exchanges: {}",
        response.unwrap_err()
    );
    let list = response.unwrap();

    assert_eq!(list.get_ref().exchange_names.len(), 3);
}

#[tokio::test]
async fn test_bind_queue() {
    let mut client = setup_server_and_client().await;

    create_queues(&mut client, &["q1".to_string()], false).await;
    create_exchanges(
        &mut client,
        &["e1".to_string()],
        ExchangeKind::Fanout,
        false,
    )
    .await;

    // ok case
    create_bindings(&mut client, &[("q1".to_string(), "e1".to_string())], false).await;

    // duplicate
    create_bindings(&mut client, &[("q1".to_string(), "e1".to_string())], true).await;

    // non-existing queue
    create_bindings(&mut client, &[("q2".to_string(), "e1".to_string())], true).await;

    // not existing exchange
    create_bindings(&mut client, &[("q1".to_string(), "e2".to_string())], true).await;

    // not existing queue and exchage
    create_bindings(&mut client, &[("q2".to_string(), "e2".to_string())], true).await;
}

#[tokio::test]
async fn test_produce_and_consume_sqsfe() {
    let mut client = setup_server_and_client().await;

    setup_sqsfe_scenario(&mut client).await;

    let payloads = produce_n_random_messages(&mut client, "e1".to_string(), 2, false).await;
    consume_messages(&mut client, "q1".to_string(), &payloads, false).await;
}

#[tokio::test]
async fn test_produce_and_consume_mqsfe() {
    let mut client = setup_server_and_client().await;

    setup_mqsfe_scenario(&mut client).await;

    let payloads = produce_n_random_messages(&mut client, "e1".to_string(), 10, false).await;
    consume_messages(&mut client, "q1".to_string(), &payloads, false).await;
    consume_messages(&mut client, "q2".to_string(), &payloads, false).await;
}

#[tokio::test]
async fn test_produce_and_consume_sqmfe() {
    let mut client = setup_server_and_client().await;

    setup_sqmfe_scenario(&mut client).await;

    let payloads1 = produce_n_random_messages(&mut client, "e1".to_string(), 10, false).await;
    let payloads2 = produce_n_random_messages(&mut client, "e2".to_string(), 10, false).await;

    consume_messages(&mut client, "q1".to_string(), &payloads1, false).await;
    consume_messages(&mut client, "q1".to_string(), &payloads2, false).await;
}

#[tokio::test]
async fn test_produce_and_consume_mqmfe() {
    let mut client = setup_server_and_client().await;

    setup_mqmfe_scenario(&mut client).await;

    let payloads1 = produce_n_random_messages(&mut client, "e1".to_string(), 10, false).await;
    let payloads2 = produce_n_random_messages(&mut client, "e2".to_string(), 10, false).await;

    consume_messages(&mut client, "q1".to_string(), &payloads1, false).await;
    consume_messages(&mut client, "q2".to_string(), &payloads1, false).await;

    consume_messages(&mut client, "q1".to_string(), &payloads2, false).await;
    consume_messages(&mut client, "q2".to_string(), &payloads2, false).await;
}
