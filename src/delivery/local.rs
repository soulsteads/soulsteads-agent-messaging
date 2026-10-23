#[cfg(feature = "agent-network")]
use super::DirectRefusal;
use super::{AgentId, ConversationKey, PeerSend, admission::AdmissionConfig};
use crate::store::Store;
use std::{
    collections::BTreeMap,
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalRefusal {
    Denied,
    NotVisible,
    DurableUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SendOutcome {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplyOutcome {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordStatus {}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecipientState {
    Attached,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct VisibleRecord {
    id: String,
    name: String,
    state: RecipientState,
}

impl VisibleRecord {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn state(&self) -> RecipientState {
        self.state
    }
}

#[derive(Clone)]
pub struct ReplyHandle {
    token: Uuid,
}

impl fmt::Debug for ReplyHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReplyHandle(..)")
    }
}

/// A logical source class. It is descriptive only; trusted registry issuance owns authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceClass {
    HumanRoot,
    PeerRequest,
    ResponseContinuation,
}

impl SourceClass {
    pub(crate) fn may_prepare_unsolicited_send(self) -> bool {
        matches!(self, Self::HumanRoot)
    }

    pub(crate) fn may_reply(self) -> bool {
        matches!(self, Self::HumanRoot | Self::PeerRequest)
    }

    pub(crate) fn admits_human_brokers(self) -> bool {
        matches!(self, Self::HumanRoot)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdmittedOrigin {
    pub(crate) platform: String,
    pub(crate) conversation: ConversationKey,
    pub(crate) room: String,
    pub(crate) event_thread: String,
    pub(crate) event_id: String,
    pub(crate) reply_to: Option<String>,
    pub(crate) matrix_raw_thread: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OwnerBinding {
    agent: AgentId,
    key: ConversationKey,
    backend: String,
    backend_id: String,
    cwd: String,
    session_id: String,
    generation: u64,
    scope_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CallerContext {
    agent: AgentId,
    key: ConversationKey,
    generation: u64,
    scope_epoch: u64,
    source: SourceClass,
    origin: AdmittedOrigin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OutputDestination {
    platform: String,
    conversation: ConversationKey,
    room: String,
    event_thread: String,
    event_id: String,
    reply_to: Option<String>,
    matrix_raw_thread: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BindingSnapshot {
    agent: AgentId,
    key: ConversationKey,
    backend: String,
    backend_id: String,
    cwd: String,
    session_id: String,
    generation: u64,
}

#[cfg(feature = "agent-network")]
pub(crate) struct DirectAdmission {
    registry: Arc<OwnerRegistry>,
    caller: CallerContext,
    store_path: PathBuf,
    recipient: BindingSnapshot,
}

#[cfg(feature = "agent-network")]
impl DirectAdmission {
    pub(crate) fn matches_registry(&self, registry: &Arc<OwnerRegistry>) -> bool {
        Arc::ptr_eq(&self.registry, registry)
    }

    pub(crate) fn is_current(&self) -> bool {
        self.registry.current(&self.caller, &self.store_path)
            && self
                .registry
                .sender_allowed(&self.caller.agent, &self.recipient.agent)
            && self
                .registry
                .binding_current(&self.recipient, &self.store_path)
    }

    pub(crate) fn platform(&self) -> &str {
        self.recipient.key.platform()
    }

    pub(crate) fn context(&self) -> &str {
        self.recipient.key.context()
    }

    pub(crate) fn backend(&self) -> &str {
        &self.recipient.backend
    }

    pub(crate) fn sender_id(&self) -> &str {
        self.caller.agent.as_str()
    }

    pub(crate) fn recipient_id(&self) -> &str {
        self.recipient.agent.as_str()
    }
}

impl From<&OwnerBinding> for BindingSnapshot {
    fn from(binding: &OwnerBinding) -> Self {
        Self {
            agent: binding.agent.clone(),
            key: binding.key.clone(),
            backend: binding.backend.clone(),
            backend_id: binding.backend_id.clone(),
            cwd: binding.cwd.clone(),
            session_id: binding.session_id.clone(),
            generation: binding.generation,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReturnRoute {
    sender: BindingSnapshot,
    expected_answerer: BindingSnapshot,
    destination: OutputDestination,
}

struct ResolvedReturnRoute {
    _source: SourceClass,
    _destination: OutputDestination,
}

#[derive(Default)]
struct RegistryState {
    by_agent: BTreeMap<AgentId, OwnerBinding>,
    by_key: BTreeMap<ConversationKey, AgentId>,
    generations: BTreeMap<AgentId, u64>,
    scope_epochs: BTreeMap<AgentId, u64>,
    return_routes: BTreeMap<Uuid, ReturnRoute>,
}

pub(crate) struct OwnerRegistry {
    config: AdmissionConfig,
    default_backend: String,
    state: Mutex<RegistryState>,
}

impl OwnerRegistry {
    pub(crate) fn new(config: AdmissionConfig, default_backend: String) -> Arc<Self> {
        Arc::new(Self {
            config,
            default_backend,
            state: Mutex::new(RegistryState::default()),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn issue_human(
        self: &Arc<Self>,
        platform: &str,
        context: &str,
        backend: &str,
        backend_id: String,
        cwd: String,
        session_id: String,
        store_path: PathBuf,
        origin: AdmittedOrigin,
    ) -> Option<LocalClient> {
        let configured = self.config.owner_at(platform, context, backend)?;
        let key = ConversationKey::new(platform, context).ok()?;
        if origin.platform != platform || origin.conversation != key {
            return None;
        }
        let mut state = self.state.lock().ok()?;
        let old = state.by_agent.get(&configured.agent).cloned();
        let unchanged = old.as_ref().is_some_and(|binding| {
            binding.key == key
                && binding.backend == backend
                && binding.backend_id == backend_id
                && binding.cwd == cwd
                && binding.session_id == session_id
        });
        let generation = if unchanged {
            old.as_ref().expect("checked").generation
        } else {
            let current = state
                .generations
                .get(&configured.agent)
                .copied()
                .unwrap_or(0);
            let next = current.checked_add(1)?;
            state.generations.insert(configured.agent.clone(), next);
            next
        };
        if !unchanged {
            state.return_routes.retain(|_, route| {
                route.sender.agent != configured.agent
                    && route.expected_answerer.agent != configured.agent
            });
            if let Some(previous) = old.as_ref()
                && previous.key != key
            {
                state.by_key.remove(&previous.key);
            }
        }
        let scope_epoch = state
            .scope_epochs
            .get(&configured.agent)
            .copied()
            .unwrap_or(0)
            .checked_add(1)?;
        state
            .scope_epochs
            .insert(configured.agent.clone(), scope_epoch);
        let binding = OwnerBinding {
            agent: configured.agent.clone(),
            key: key.clone(),
            backend: backend.to_owned(),
            backend_id,
            cwd,
            session_id,
            generation,
            scope_epoch,
        };
        state.by_key.insert(key.clone(), configured.agent.clone());
        state.by_agent.insert(configured.agent.clone(), binding);
        let caller = CallerContext {
            agent: configured.agent.clone(),
            key,
            generation,
            scope_epoch,
            source: SourceClass::HumanRoot,
            origin,
        };
        Some(LocalClient {
            inner: Arc::new(LocalClientInner {
                registry: Arc::downgrade(self),
                caller,
                store_path,
            }),
        })
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn issue(
        self: &Arc<Self>,
        platform: &str,
        context: &str,
        backend: &str,
        backend_id: String,
        cwd: String,
        session_id: String,
        store_path: PathBuf,
    ) -> Option<LocalClient> {
        let key = ConversationKey::new(platform, context).ok()?;
        self.issue_human(
            platform,
            context,
            backend,
            backend_id,
            cwd,
            session_id,
            store_path,
            AdmittedOrigin {
                platform: platform.to_owned(),
                conversation: key,
                room: context.to_owned(),
                event_thread: String::new(),
                event_id: String::new(),
                reply_to: None,
                matrix_raw_thread: None,
            },
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn issue_non_human_for_test(
        self: &Arc<Self>,
        source: SourceClass,
        platform: &str,
        context: &str,
        backend: &str,
        backend_id: String,
        cwd: String,
        session_id: String,
        store_path: PathBuf,
    ) -> Option<LocalClient> {
        if source == SourceClass::HumanRoot {
            return None;
        }
        let human = self.issue(
            platform, context, backend, backend_id, cwd, session_id, store_path,
        )?;
        let mut caller = human.inner.caller.clone();
        caller.source = source;
        Some(LocalClient {
            inner: Arc::new(LocalClientInner {
                registry: Arc::downgrade(self),
                caller,
                store_path: human.inner.store_path.clone(),
            }),
        })
    }

    pub(crate) fn revoke(&self, platform: &str, context: &str) {
        let Ok(key) = ConversationKey::new(platform, context) else {
            return;
        };
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Some(agent) = state.by_key.remove(&key) {
            state.by_agent.remove(&agent);
            state.return_routes.retain(|_, route| {
                route.sender.agent != agent && route.expected_answerer.agent != agent
            });
            if let Some(next) = state
                .generations
                .get(&agent)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
            {
                state.generations.insert(agent, next);
            }
        }
    }

    pub(crate) fn revoke_all(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let agents = state.by_agent.keys().cloned().collect::<Vec<_>>();
        state.by_agent.clear();
        state.by_key.clear();
        state.return_routes.clear();
        for agent in agents {
            if let Some(next) = state
                .generations
                .get(&agent)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
            {
                state.generations.insert(agent, next);
            }
        }
    }

    fn current(&self, caller: &CallerContext, store_path: &PathBuf) -> bool {
        let Ok(state) = self.state.lock() else {
            return false;
        };
        let Some(binding) = state.by_agent.get(&caller.agent) else {
            return false;
        };
        if binding.generation != caller.generation
            || binding.key != caller.key
            || binding.scope_epoch != caller.scope_epoch
        {
            return false;
        }
        let snapshot = BindingSnapshot::from(binding);
        drop(state);
        self.binding_current(&snapshot, store_path)
    }

    fn binding_current(&self, snapshot: &BindingSnapshot, store_path: &PathBuf) -> bool {
        let Ok(state) = self.state.lock() else {
            return false;
        };
        let Some(binding) = state.by_agent.get(&snapshot.agent) else {
            return false;
        };
        if BindingSnapshot::from(binding) != *snapshot {
            return false;
        }
        drop(state);
        let store = Store::new(store_path);
        let Ok(selected) = store.selected(
            snapshot.key.platform(),
            snapshot.key.context(),
            &self.default_backend,
        ) else {
            return false;
        };
        if selected != snapshot.backend {
            return false;
        }
        matches!(
            store.get(snapshot.key.platform(), snapshot.key.context(), &snapshot.backend),
            Ok(Some(entry)) if entry.backend == snapshot.backend
                && entry.backend_id == snapshot.backend_id
                && entry.cwd == snapshot.cwd
                && entry.session_id == snapshot.session_id
        )
    }

    fn recipient_is_current(&self, agent: &AgentId, store_path: &PathBuf) -> bool {
        let Ok(state) = self.state.lock() else {
            return false;
        };
        let Some(binding) = state.by_agent.get(agent) else {
            return false;
        };
        let snapshot = BindingSnapshot::from(binding);
        drop(state);
        self.binding_current(&snapshot, store_path)
    }

    fn sender_allowed(&self, sender: &AgentId, recipient: &AgentId) -> bool {
        self.config
            .owner(recipient)
            .is_some_and(|owner| owner.allowed_senders.contains(sender))
    }

    fn visible_recipients(
        &self,
        caller: &CallerContext,
        store_path: &PathBuf,
    ) -> Option<Vec<VisibleRecord>> {
        if caller.source != SourceClass::HumanRoot || !self.current(caller, store_path) {
            return None;
        }
        Some(
            self.config
                .owners()
                .filter(|owner| {
                    owner.agent != caller.agent && owner.allowed_senders.contains(&caller.agent)
                })
                .map(|owner| VisibleRecord {
                    id: owner.agent.as_str().to_owned(),
                    name: owner.name.clone(),
                    state: if self.recipient_is_current(&owner.agent, store_path) {
                        RecipientState::Attached
                    } else {
                        RecipientState::Unavailable
                    },
                })
                .collect(),
        )
    }

    #[cfg(feature = "agent-network")]
    fn admit_direct(
        self: &Arc<Self>,
        caller: &CallerContext,
        store_path: &PathBuf,
        recipient: &AgentId,
    ) -> Option<DirectAdmission> {
        if caller.source != SourceClass::HumanRoot
            || caller.agent == *recipient
            || !self.current(caller, store_path)
            || !self.sender_allowed(&caller.agent, recipient)
        {
            return None;
        }
        let recipient = self.state.lock().ok()?.by_agent.get(recipient)?.clone();
        let admission = DirectAdmission {
            registry: self.clone(),
            caller: caller.clone(),
            store_path: store_path.clone(),
            recipient: BindingSnapshot::from(&recipient),
        };
        admission.is_current().then_some(admission)
    }

    fn prepare_return_route(
        &self,
        sender: &CallerContext,
        expected_answerer: &AgentId,
        store_path: &PathBuf,
    ) -> Option<ReplyHandle> {
        if !sender.source.may_prepare_unsolicited_send()
            || sender.agent == *expected_answerer
            || !self.current(sender, store_path)
            || !self.sender_allowed(&sender.agent, expected_answerer)
            || !self.recipient_is_current(expected_answerer, store_path)
        {
            return None;
        }
        let mut state = self.state.lock().ok()?;
        let sender_binding = state.by_agent.get(&sender.agent)?.clone();
        if sender_binding.key != sender.key
            || sender_binding.generation != sender.generation
            || sender_binding.scope_epoch != sender.scope_epoch
        {
            return None;
        }
        let answerer_binding = state.by_agent.get(expected_answerer)?.clone();
        let token = Uuid::new_v4();
        state.return_routes.insert(
            token,
            ReturnRoute {
                sender: BindingSnapshot::from(&sender_binding),
                expected_answerer: BindingSnapshot::from(&answerer_binding),
                destination: OutputDestination {
                    platform: sender.origin.platform.clone(),
                    conversation: sender.origin.conversation.clone(),
                    room: sender.origin.room.clone(),
                    event_thread: sender.origin.event_thread.clone(),
                    event_id: sender.origin.event_id.clone(),
                    reply_to: sender.origin.reply_to.clone(),
                    matrix_raw_thread: sender.origin.matrix_raw_thread.clone(),
                },
            },
        );
        Some(ReplyHandle { token })
    }

    fn resolve_return_route(
        &self,
        answerer: &CallerContext,
        handle: &ReplyHandle,
        store_path: &PathBuf,
    ) -> Option<ResolvedReturnRoute> {
        if !answerer.source.may_reply() || !self.current(answerer, store_path) {
            return None;
        }
        let route = self
            .state
            .lock()
            .ok()?
            .return_routes
            .get(&handle.token)?
            .clone();
        if route.expected_answerer.agent != answerer.agent
            || !self.sender_allowed(&route.sender.agent, &answerer.agent)
            || !self.binding_current(&route.sender, store_path)
            || !self.binding_current(&route.expected_answerer, store_path)
        {
            return None;
        }
        Some(ResolvedReturnRoute {
            _source: answerer.source,
            _destination: route.destination,
        })
    }

    fn revoke_return_route(&self, handle: &ReplyHandle) {
        if let Ok(mut state) = self.state.lock() {
            state.return_routes.remove(&handle.token);
        }
    }

    #[cfg(test)]
    fn return_route_count(&self) -> usize {
        self.state
            .lock()
            .map(|state| state.return_routes.len())
            .unwrap_or(0)
    }
}

struct LocalClientInner {
    registry: Weak<OwnerRegistry>,
    caller: CallerContext,
    store_path: PathBuf,
}

#[derive(Clone)]
pub struct LocalClient {
    inner: Arc<LocalClientInner>,
}

impl LocalClient {
    #[cfg(feature = "agent-network")]
    pub(crate) fn admit_direct(
        &self,
        recipient: &AgentId,
    ) -> Result<DirectAdmission, DirectRefusal> {
        let Some(registry) = self.inner.registry.upgrade() else {
            return Err(DirectRefusal::Denied);
        };
        registry
            .admit_direct(&self.inner.caller, &self.inner.store_path, recipient)
            .ok_or(DirectRefusal::Denied)
    }

    pub async fn list_visible(&self) -> Result<Vec<VisibleRecord>, LocalRefusal> {
        let Some(registry) = self.inner.registry.upgrade() else {
            return Err(LocalRefusal::NotVisible);
        };
        registry
            .visible_recipients(&self.inner.caller, &self.inner.store_path)
            .ok_or(LocalRefusal::NotVisible)
    }

    pub async fn send(&self, send: PeerSend) -> Result<SendOutcome, LocalRefusal> {
        let Some(registry) = self.inner.registry.upgrade() else {
            return Err(LocalRefusal::Denied);
        };
        if !self.inner.caller.source.may_prepare_unsolicited_send()
            || !registry.current(&self.inner.caller, &self.inner.store_path)
            || !registry.sender_allowed(&self.inner.caller.agent, send.recipient())
            || !registry.recipient_is_current(send.recipient(), &self.inner.store_path)
        {
            return Err(LocalRefusal::Denied);
        }
        let Some(route) = registry.prepare_return_route(
            &self.inner.caller,
            send.recipient(),
            &self.inner.store_path,
        ) else {
            return Err(LocalRefusal::Denied);
        };
        // P4 must publish a pending row and this route atomically. P3 has no durable
        // authority, so the preparatory capability cannot escape this unavailable call.
        registry.revoke_return_route(&route);
        Err(LocalRefusal::DurableUnavailable)
    }

    pub async fn reply(
        &self,
        route: ReplyHandle,
        _text: &str,
        _idempotency_key: &str,
    ) -> Result<ReplyOutcome, LocalRefusal> {
        let Some(registry) = self.inner.registry.upgrade() else {
            return Err(LocalRefusal::Denied);
        };
        if registry
            .resolve_return_route(&self.inner.caller, &route, &self.inner.store_path)
            .is_none()
        {
            return Err(LocalRefusal::Denied);
        }
        // A resolved P3 route is authorization only. P4 owns durable acceptance;
        // revoke this ephemeral preparation rather than fabricate a receipt.
        registry.revoke_return_route(&route);
        Err(LocalRefusal::DurableUnavailable)
    }

    pub async fn status(&self, _message_id: &str) -> Result<RecordStatus, LocalRefusal> {
        let Some(registry) = self.inner.registry.upgrade() else {
            return Err(LocalRefusal::NotVisible);
        };
        if !registry.current(&self.inner.caller, &self.inner.store_path) {
            return Err(LocalRefusal::NotVisible);
        }
        Err(LocalRefusal::DurableUnavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn registry() -> Arc<OwnerRegistry> {
        let config = AdmissionConfig::from_value(&json!({
            "agentMessaging": {"enabled": true, "agents": [
                {"id":"alpha","name":"Alpha","target":{"platform":"web","context":"alpha","backend":"opencode"},"allowedSenders":[]},
                {"id":"beta","name":"Beta","target":{"platform":"web","context":"beta","backend":"opencode"},"allowedSenders":["alpha"]}
            ]}
        }))
        .unwrap()
        .unwrap();
        OwnerRegistry::new(config, "opencode".into())
    }

    fn entry(context: &str, session: &str) -> crate::store::Entry {
        crate::store::Entry {
            connector: "web".into(),
            thread_id: context.into(),
            session_id: session.into(),
            cwd: "/trusted".into(),
            backend_id: "backend-id".into(),
            updated_at: "now".into(),
            backend: "opencode".into(),
        }
    }

    #[tokio::test]
    async fn authorized_reads_report_durable_unavailable_and_revoked_reads_are_not_visible() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.json");
        let store = Store::new(&path);
        store.set(entry("alpha", "session-a"), true).unwrap();
        let registry = registry();
        let alpha = registry
            .issue(
                "web",
                "alpha",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-a".into(),
                path.clone(),
            )
            .unwrap();

        let beta = registry
            .issue(
                "web",
                "beta",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-b".into(),
                path.clone(),
            )
            .unwrap();
        store.set(entry("beta", "session-b"), true).unwrap();
        let roster = alpha.list_visible().await.unwrap();
        assert_eq!(roster.len(), 1);
        assert_eq!(roster[0].id(), "beta");
        assert_eq!(roster[0].name(), "Beta");
        assert_eq!(roster[0].state(), RecipientState::Attached);
        assert_eq!(
            serde_json::to_value(&roster[0]).unwrap(),
            json!({
                "id": "beta", "name": "Beta", "state": "attached"
            })
        );
        assert_eq!(
            beta.list_visible().await.unwrap(),
            Vec::<VisibleRecord>::new()
        );
        registry.revoke("web", "beta");
        let unavailable = alpha.list_visible().await.unwrap();
        assert_eq!(unavailable[0].state(), RecipientState::Unavailable);
        assert_eq!(
            alpha.status("missing").await,
            Err(LocalRefusal::DurableUnavailable)
        );
        registry.revoke("web", "alpha");
        assert_eq!(alpha.list_visible().await, Err(LocalRefusal::NotVisible));
        assert_eq!(alpha.status("missing").await, Err(LocalRefusal::NotVisible));
    }

    #[tokio::test]
    async fn binding_and_recipient_authority_negatives_refuse_without_durable_delivery() {
        for tamper in [
            "caller session",
            "caller cwd",
            "caller backend id",
            "caller selected backend",
            "recipient session",
            "recipient cwd",
            "recipient backend id",
            "recipient selected backend",
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("sessions.json");
            let store = Store::new(&path);
            store.set(entry("alpha", "session-a"), true).unwrap();
            store.set(entry("beta", "session-b"), true).unwrap();
            let registry = registry();
            let alpha = registry
                .issue(
                    "web",
                    "alpha",
                    "opencode",
                    "backend-id".into(),
                    "/trusted".into(),
                    "session-a".into(),
                    path.clone(),
                )
                .unwrap();
            registry
                .issue(
                    "web",
                    "beta",
                    "opencode",
                    "backend-id".into(),
                    "/trusted".into(),
                    "session-b".into(),
                    path.clone(),
                )
                .unwrap();
            let target = if tamper.starts_with("caller") {
                "alpha"
            } else {
                "beta"
            };
            if tamper.ends_with("selected backend") {
                store.set_selected("web", target, "other").unwrap();
            } else {
                let mut changed = entry(
                    target,
                    if target == "alpha" {
                        "session-a"
                    } else {
                        "session-b"
                    },
                );
                match tamper {
                    "caller session" | "recipient session" => {
                        changed.session_id = "tampered".into()
                    }
                    "caller cwd" | "recipient cwd" => changed.cwd = "/tampered".into(),
                    "caller backend id" | "recipient backend id" => {
                        changed.backend_id = "tampered".into()
                    }
                    _ => unreachable!(),
                }
                store.set(changed, false).unwrap();
            }
            let send = PeerSend::new(AgentId::try_from("beta").unwrap(), tamper, "data").unwrap();
            assert_eq!(
                alpha.send(send).await,
                Err(LocalRefusal::Denied),
                "{tamper}"
            );
        }
    }

    #[tokio::test]
    async fn policy_and_weak_registry_negatives_refuse_old_or_wrong_contexts() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.json");
        let store = Store::new(&path);
        store.set(entry("alpha", "session-a"), true).unwrap();
        store.set(entry("beta", "session-b"), true).unwrap();
        let registry = registry();
        let alpha = registry
            .issue(
                "web",
                "alpha",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-a".into(),
                path.clone(),
            )
            .unwrap();
        let beta = registry
            .issue(
                "web",
                "beta",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-b".into(),
                path.clone(),
            )
            .unwrap();
        let send = |recipient| {
            PeerSend::new(AgentId::try_from(recipient).unwrap(), "request", "data").unwrap()
        };

        assert_eq!(
            alpha.send(send("beta")).await,
            Err(LocalRefusal::DurableUnavailable)
        );
        assert_eq!(alpha.send(send("alpha")).await, Err(LocalRefusal::Denied));
        assert_eq!(alpha.send(send("unknown")).await, Err(LocalRefusal::Denied));
        let route = registry
            .prepare_return_route(
                &alpha.inner.caller,
                &AgentId::try_from("beta").unwrap(),
                &path,
            )
            .expect("production route factory");
        assert_eq!(
            beta.reply(route.clone(), "answer", "route").await,
            Err(LocalRefusal::DurableUnavailable)
        );
        registry.revoke("web", "alpha");
        assert_eq!(
            beta.reply(route, "answer", "route").await,
            Err(LocalRefusal::Denied)
        );
        drop(registry);
        assert_eq!(alpha.send(send("beta")).await, Err(LocalRefusal::Denied));
    }

    #[tokio::test]
    async fn production_routes_resolve_only_for_current_expected_owner_and_revoke() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.json");
        let store = Store::new(&path);
        store.set(entry("alpha", "session-a"), true).unwrap();
        store.set(entry("beta", "session-b"), true).unwrap();
        let registry = registry();
        let mut alpha = registry
            .issue(
                "web",
                "alpha",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-a".into(),
                path.clone(),
            )
            .unwrap();
        let _beta = registry
            .issue(
                "web",
                "beta",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-b".into(),
                path.clone(),
            )
            .unwrap();
        let route = registry
            .prepare_return_route(
                &alpha.inner.caller,
                &AgentId::try_from("beta").unwrap(),
                &path,
            )
            .expect("production route factory");
        let continuation = registry
            .issue_non_human_for_test(
                SourceClass::ResponseContinuation,
                "web",
                "beta",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-b".into(),
                path.clone(),
            )
            .unwrap();
        assert_eq!(
            continuation
                .reply(route.clone(), "answer", "continuation")
                .await,
            Err(LocalRefusal::Denied)
        );
        let beta = registry
            .issue(
                "web",
                "beta",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-b".into(),
                path.clone(),
            )
            .unwrap();
        let cloned = route.clone();

        assert!(
            registry
                .resolve_return_route(&beta.inner.caller, &route, &path)
                .is_some()
        );
        assert!(
            registry
                .resolve_return_route(&alpha.inner.caller, &route, &path)
                .is_none()
        );
        registry.revoke_return_route(&route);
        assert!(
            registry
                .resolve_return_route(&beta.inner.caller, &cloned, &path)
                .is_none()
        );

        for revoked in ["alpha", "beta"] {
            let route = registry
                .prepare_return_route(
                    &alpha.inner.caller,
                    &AgentId::try_from("beta").unwrap(),
                    &path,
                )
                .expect("production route factory");
            registry.revoke("web", revoked);
            assert!(
                registry
                    .resolve_return_route(&beta.inner.caller, &route, &path)
                    .is_none()
            );
            if revoked == "alpha" {
                alpha = registry
                    .issue(
                        "web",
                        "alpha",
                        "opencode",
                        "backend-id".into(),
                        "/trusted".into(),
                        "session-a".into(),
                        path.clone(),
                    )
                    .unwrap();
            }
            if revoked == "beta" {
                let _ = registry.issue(
                    "web",
                    "beta",
                    "opencode",
                    "backend-id".into(),
                    "/trusted".into(),
                    "session-b".into(),
                    path.clone(),
                );
            }
        }
    }

    #[tokio::test]
    async fn unavailable_send_drops_its_preparatory_production_route() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.json");
        let store = Store::new(&path);
        store.set(entry("alpha", "session-a"), true).unwrap();
        store.set(entry("beta", "session-b"), true).unwrap();
        let registry = registry();
        let alpha = registry
            .issue(
                "web",
                "alpha",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-a".into(),
                path.clone(),
            )
            .unwrap();
        let _beta = registry
            .issue(
                "web",
                "beta",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-b".into(),
                path,
            )
            .unwrap();

        assert_eq!(registry.return_route_count(), 0);
        assert_eq!(
            alpha
                .send(PeerSend::new(AgentId::try_from("beta").unwrap(), "request", "data").unwrap())
                .await,
            Err(LocalRefusal::DurableUnavailable)
        );
        assert_eq!(registry.return_route_count(), 0);
    }

    #[tokio::test]
    async fn fresh_human_scope_expires_old_client_without_revoking_its_bound_return_route() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.json");
        let store = Store::new(&path);
        store.set(entry("alpha", "session-a"), true).unwrap();
        store.set(entry("beta", "session-b"), true).unwrap();
        let registry = registry();
        let old_alpha = registry
            .issue(
                "web",
                "alpha",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-a".into(),
                path.clone(),
            )
            .unwrap();
        let beta = registry
            .issue(
                "web",
                "beta",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-b".into(),
                path.clone(),
            )
            .unwrap();
        let route = registry
            .prepare_return_route(
                &old_alpha.inner.caller,
                &AgentId::try_from("beta").unwrap(),
                &path,
            )
            .expect("production route factory");
        let fresh_alpha = registry
            .issue(
                "web",
                "alpha",
                "opencode",
                "backend-id".into(),
                "/trusted".into(),
                "session-a".into(),
                path.clone(),
            )
            .unwrap();

        assert_eq!(
            old_alpha.inner.caller.generation,
            fresh_alpha.inner.caller.generation
        );
        assert_eq!(
            old_alpha
                .send(PeerSend::new(AgentId::try_from("beta").unwrap(), "old", "data").unwrap())
                .await,
            Err(LocalRefusal::Denied)
        );
        assert_eq!(
            fresh_alpha
                .send(PeerSend::new(AgentId::try_from("beta").unwrap(), "fresh", "data").unwrap())
                .await,
            Err(LocalRefusal::DurableUnavailable)
        );
        assert!(
            registry
                .resolve_return_route(&beta.inner.caller, &route, &path)
                .is_some()
        );
    }

    #[tokio::test]
    async fn sealed_source_capabilities_only_allow_human_roots_to_prepare_unsolicited_sends() {
        for (source, may_prepare, may_reply, admits_human_brokers) in [
            (SourceClass::HumanRoot, true, true, true),
            (SourceClass::PeerRequest, false, true, false),
            (SourceClass::ResponseContinuation, false, false, false),
        ] {
            assert_eq!(
                source.may_prepare_unsolicited_send(),
                may_prepare,
                "{source:?}"
            );
            assert_eq!(source.may_reply(), may_reply, "{source:?}");
            assert_eq!(
                source.admits_human_brokers(),
                admits_human_brokers,
                "{source:?}"
            );
        }
        for source in [SourceClass::PeerRequest, SourceClass::ResponseContinuation] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("sessions.json");
            let store = Store::new(&path);
            store.set(entry("alpha", "session-a"), true).unwrap();
            store.set(entry("beta", "session-b"), true).unwrap();
            let registry = registry();
            let peer = registry
                .issue_non_human_for_test(
                    source,
                    "web",
                    "alpha",
                    "opencode",
                    "backend-id".into(),
                    "/trusted".into(),
                    "session-a".into(),
                    path.clone(),
                )
                .unwrap();
            let _beta = registry
                .issue(
                    "web",
                    "beta",
                    "opencode",
                    "backend-id".into(),
                    "/trusted".into(),
                    "session-b".into(),
                    path,
                )
                .unwrap();
            assert_eq!(
                peer.send(
                    PeerSend::new(AgentId::try_from("beta").unwrap(), "peer", "data").unwrap()
                )
                .await,
                Err(LocalRefusal::Denied),
                "{source:?} must not become a PeerSend authority"
            );
        }
    }
}
