#![cfg(feature = "a2a")]

use a2a_core::{
    AgentCapabilities, AgentCard, AgentInterface, Artifact, CancelTaskRequest, GetTaskRequest,
    Message, Part, Role, SendMessageRequest, StreamResponse, TaskArtifactUpdateEvent, TaskState,
    TaskStatus, TaskStatusUpdateEvent,
};
use a2a_server::{
    AgentExecutor, DefaultRequestHandler, ExecutorContext, InMemoryTaskStore, StaticAgentCard,
};
use a2a_server::{agent_card::agent_card_router, jsonrpc::jsonrpc_router};
use futures::{StreamExt, stream};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle, time::timeout};

struct Loopback {
    url: String,
    task: JoinHandle<()>,
}
impl Drop for Loopback {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server<E: AgentExecutor>(executor: E) -> (Loopback, AgentCard) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");
    let card = AgentCard {
        name: "SDK smoke agent".into(),
        description: "deterministic test fixture".into(),
        version: "1".into(),
        supported_interfaces: vec![AgentInterface::new(format!("{url}/"), "JSONRPC")],
        capabilities: AgentCapabilities {
            streaming: Some(true),
            ..Default::default()
        },
        default_input_modes: vec!["text".into()],
        default_output_modes: vec!["text".into()],
        skills: vec![],
        provider: None,
        documentation_url: None,
        icon_url: None,
        security_schemes: None,
        security_requirements: None,
        signatures: None,
    };
    let card_router = agent_card_router(Arc::new(StaticAgentCard::new(card.clone())));
    let handler = Arc::new(DefaultRequestHandler::new(
        executor,
        InMemoryTaskStore::new(),
    ));
    let app = card_router.merge(jsonrpc_router(handler));
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (Loopback { url, task }, card)
}
#[derive(Clone)]
struct DeterministicExecutor {
    release: Arc<tokio::sync::Mutex<Option<oneshot::Receiver<()>>>>,
}
impl AgentExecutor for DeterministicExecutor {
    fn execute(
        &self,
        ctx: ExecutorContext,
    ) -> futures::stream::BoxStream<'static, Result<StreamResponse, a2a_core::A2AError>> {
        let task_id = ctx.task_id;
        let context_id = ctx.context_id;
        let working = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
            task_id: task_id.clone(),
            context_id: context_id.clone(),
            status: TaskStatus {
                state: TaskState::Working,
                message: None,
                timestamp: None,
            },
            metadata: None,
        });
        let release = self.release.clone();
        let final_task_id = task_id.clone();
        let final_context_id = context_id.clone();
        Box::pin(
            stream::once(async move { Ok(working) })
                .chain(stream::once(async move {
                    if let Some(rx) = release.lock().await.take() {
                        let _ = rx.await;
                    }
                    Ok(StreamResponse::ArtifactUpdate(TaskArtifactUpdateEvent {
                        task_id: task_id.clone(),
                        context_id: context_id.clone(),
                        artifact: Artifact {
                            artifact_id: "answer".into(),
                            name: None,
                            description: None,
                            parts: vec![Part::text("hello from SDK")],
                            extensions: None,
                            metadata: None,
                        },
                        append: None,
                        last_chunk: Some(true),
                        metadata: None,
                    }))
                }))
                .chain(stream::once(async move {
                    Ok(StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
                        task_id: final_task_id,
                        context_id: final_context_id,
                        status: TaskStatus {
                            state: TaskState::Completed,
                            message: None,
                            timestamp: None,
                        },
                        metadata: None,
                    }))
                })),
        )
    }
    fn cancel(
        &self,
        _ctx: ExecutorContext,
    ) -> futures::stream::BoxStream<'static, Result<StreamResponse, a2a_core::A2AError>> {
        Box::pin(stream::empty())
    }
}
fn executor() -> (DeterministicExecutor, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    (
        DeterministicExecutor {
            release: Arc::new(tokio::sync::Mutex::new(Some(rx))),
        },
        tx,
    )
}
async fn discover(url: &str) -> AgentCard {
    timeout(Duration::from_secs(5), async {
        reqwest::get(format!("{url}/.well-known/agent-card.json"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    })
    .await
    .unwrap()
}

async fn drain_qualification_events(
    events: &mut (dyn futures::Stream<Item = Result<StreamResponse, a2a_core::A2AError>> + Unpin),
) -> Result<Vec<&'static str>, &'static str> {
    let mut kinds = vec!["working"];
    let artifact = timeout(Duration::from_secs(5), events.next())
        .await
        .map_err(|_| "event timeout")?
        .ok_or("missing artifact")?
        .map_err(|_| "stream error")?;
    match artifact {
        StreamResponse::ArtifactUpdate(e) => {
            if e.artifact.parts.first().and_then(|part| part.as_text()) != Some("hello from SDK") {
                return Err("unexpected artifact");
            }
            kinds.push("artifact");
        }
        _ => return Err("expected artifact"),
    }

    let completed = timeout(Duration::from_secs(5), events.next())
        .await
        .map_err(|_| "event timeout")?
        .ok_or("missing completed status")?
        .map_err(|_| "stream error")?;
    match completed {
        StreamResponse::StatusUpdate(e) if e.status.state == TaskState::Completed => {
            kinds.push("completed");
        }
        _ => return Err("expected completed status"),
    }

    match timeout(Duration::from_secs(5), events.next())
        .await
        .map_err(|_| "event timeout")?
    {
        None => Ok(kinds),
        Some(Err(_)) => Err("stream error"),
        Some(Ok(_)) => Err("unexpected extra event"),
    }
}

#[tokio::test]
async fn qualification_drain_rejects_duplicate_working_event() {
    let task_id = "task-1".to_owned();
    let context_id = "context-1".to_owned();
    let working = || {
        StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
            task_id: task_id.clone(),
            context_id: context_id.clone(),
            status: TaskStatus {
                state: TaskState::Working,
                message: None,
                timestamp: None,
            },
            metadata: None,
        })
    };
    let artifact = StreamResponse::ArtifactUpdate(TaskArtifactUpdateEvent {
        task_id: task_id.clone(),
        context_id: context_id.clone(),
        artifact: Artifact {
            artifact_id: "answer".into(),
            name: None,
            description: None,
            parts: vec![Part::text("hello from SDK")],
            extensions: None,
            metadata: None,
        },
        append: None,
        last_chunk: Some(true),
        metadata: None,
    });
    let completed = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
        task_id: task_id.clone(),
        context_id: context_id.clone(),
        status: TaskStatus {
            state: TaskState::Completed,
            message: None,
            timestamp: None,
        },
        metadata: None,
    });
    let mut events = Box::pin(stream::iter(vec![
        Ok(working()),
        Ok(artifact),
        Ok(completed),
    ]));
    assert!(drain_qualification_events(&mut events).await.is_err());
}

#[tokio::test]
async fn published_client_discovers_sends_and_gets_task() {
    let (live, _) = server(executor().0).await;
    let card = discover(&live.url).await;
    assert_eq!(card.name, "SDK smoke agent");
    let c = a2a_client::A2AClientFactory::builder()
        .build()
        .create_from_card(&card)
        .await
        .unwrap();
    let response = timeout(
        Duration::from_secs(5),
        c.send_message(&SendMessageRequest {
            message: Message::new(Role::User, vec![Part::text("request")]),
            configuration: None,
            metadata: None,
            tenant: None,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let task = match response {
        a2a_core::SendMessageResponse::Task(task) => task,
        _ => panic!("expected Task"),
    };
    assert_eq!(task.status.state, TaskState::Completed);
    assert_eq!(
        task.artifacts.as_ref().unwrap()[0].parts[0].as_text(),
        Some("hello from SDK")
    );
    let fetched = timeout(
        Duration::from_secs(5),
        c.get_task(&GetTaskRequest {
            id: task.id.clone(),
            history_length: None,
            tenant: None,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fetched.id, task.id);
}
#[tokio::test]
async fn published_client_streams_in_order_after_terminal_barrier() {
    let (ex, release) = executor();
    let (live, _) = server(ex).await;
    let card = discover(&live.url).await;
    let c = a2a_client::A2AClientFactory::builder()
        .build()
        .create_from_card(&card)
        .await
        .unwrap();
    let mut events = timeout(
        Duration::from_secs(5),
        c.send_streaming_message(&SendMessageRequest {
            message: Message::new(Role::User, vec![Part::text("stream")]),
            configuration: None,
            metadata: None,
            tenant: None,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let first = timeout(Duration::from_secs(5), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let id = match first {
        StreamResponse::StatusUpdate(e) => {
            assert_eq!(e.status.state, TaskState::Working);
            e.task_id
        }
        _ => panic!("expected working status"),
    };
    let fetched = timeout(
        Duration::from_secs(5),
        c.get_task(&GetTaskRequest {
            id: id.clone(),
            history_length: None,
            tenant: None,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fetched.status.state, TaskState::Working);
    release.send(()).unwrap();
    let kinds = drain_qualification_events(&mut events).await.unwrap();
    assert_eq!(kinds, ["working", "artifact", "completed"]);
    assert_eq!(
        timeout(
            Duration::from_secs(5),
            c.get_task(&GetTaskRequest {
                id,
                history_length: None,
                tenant: None
            })
        )
        .await
        .unwrap()
        .unwrap()
        .status
        .state,
        TaskState::Completed
    );
}
#[tokio::test]
async fn published_client_cancel_active_task_yields_canceled_state() {
    let (ex, _release) = executor();
    let (live, _) = server(ex).await;
    let card = discover(&live.url).await;
    let c = a2a_client::A2AClientFactory::builder()
        .build()
        .create_from_card(&card)
        .await
        .unwrap();
    let mut events = timeout(
        Duration::from_secs(5),
        c.send_streaming_message(&SendMessageRequest {
            message: Message::new(Role::User, vec![Part::text("active")]),
            configuration: None,
            metadata: None,
            tenant: None,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let first = timeout(Duration::from_secs(5), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let id = match first {
        StreamResponse::StatusUpdate(e) => {
            assert_eq!(e.status.state, TaskState::Working);
            e.task_id
        }
        _ => panic!("expected working status"),
    };
    let result = timeout(
        Duration::from_secs(5),
        c.cancel_task(&CancelTaskRequest {
            id,
            tenant: None,
            metadata: None,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.status.state, TaskState::Canceled);
}
