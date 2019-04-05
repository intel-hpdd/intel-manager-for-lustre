// Copyright (c) 2019 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use futures::prelude::*;
use iml_rabbit::{
    basic_consume, create_channel, declare_transient_queue, TcpClient, TcpStreamConsumerFuture,
};
use iml_wire_types::{Fqdn, Id, ManagerMessage, Message, PluginMessage, PluginName, Seq};
use lapin_futures::channel::BasicConsumeOptions;

pub struct AgentData {
    pub fqdn: Fqdn,
    pub plugin: PluginName,
    pub session_id: Id,
    pub session_seq: Seq,
    pub body: serde_json::Value,
}

impl std::fmt::Display for AgentData {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:?}/{:?}/{:?}-{:?}",
            self.fqdn, self.plugin, self.session_id, self.session_seq,
        )
    }
}

/// Converts agent Message out of it's enum and into a discrete AgentData
/// struct. This function will panic if the Message is not Data.
impl From<Message> for AgentData {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Data {
                fqdn,
                plugin,
                session_id,
                session_seq,
                body,
                ..
            } => AgentData {
                fqdn,
                plugin,
                session_id,
                session_seq,
                body,
            },
            _ => panic!("Cannot convert to AgentData, Message was not Data"),
        }
    }
}

impl From<AgentData> for PluginMessage {
    fn from(
        AgentData {
            fqdn,
            plugin,
            session_id,
            session_seq,
            body,
        }: AgentData,
    ) -> Self {
        PluginMessage::Data {
            fqdn,
            plugin,
            session_id,
            session_seq,
            body,
        }
    }
}

pub fn terminate_agent_session(
    plugin: &PluginName,
    fqdn: &Fqdn,
    session_id: Id,
    client: TcpClient,
) -> impl Future<Item = TcpClient, Error = failure::Error> {
    iml_manager_messaging::send_agent_message(
        client,
        ManagerMessage::SessionTerminate {
            fqdn: fqdn.clone(),
            plugin: plugin.clone(),
            session_id,
        },
    )
}

pub fn consume_agent_tx_queue() -> impl TcpStreamConsumerFuture {
    iml_rabbit::connect_to_rabbit()
        .and_then(create_channel)
        .and_then(|ch| declare_transient_queue("agent_tx_rust".to_string(), ch))
        .and_then(|(ch, q)| {
            basic_consume(
                ch,
                q,
                "agent_tx_rust",
                Some(BasicConsumeOptions {
                    no_ack: true,
                    exclusive: true,
                    ..Default::default()
                }),
            )
        })
}
