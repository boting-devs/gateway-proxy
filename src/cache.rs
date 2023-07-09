#[cfg(feature = "simd-json")]
use halfbrown::hashmap;
use serde::Serialize;
#[cfg(not(feature = "simd-json"))]
use serde_json::Value as OwnedValue;
#[cfg(feature = "simd-json")]
use simd_json::OwnedValue;
use twilight_cache_inmemory::{InMemoryCache, InMemoryCacheStats, UpdateCache};
use twilight_model::{
    channel::Channel,
    gateway::{
        payload::incoming::{GuildCreate, GuildDelete},
        OpCode,
    },
    guild::{Guild, Member, Role},
    id::{
        marker::{GuildMarker, UserMarker},
        Id,
    },
    voice::VoiceState,
};

use std::sync::Arc;

use crate::model::JsonObject;

#[derive(Serialize)]
pub struct Payload {
    pub d: Event,
    pub op: OpCode,
    pub t: String,
    pub s: usize,
}

#[derive(Serialize, Clone)]
#[serde(untagged)]
pub enum Event {
    Ready(JsonObject),
    GuildCreate(Box<GuildCreate>),
    GuildDelete(GuildDelete),
}

pub struct Guilds(Arc<InMemoryCache>, u32);

impl Guilds {
    pub fn new(cache: Arc<InMemoryCache>, shard_id: u32) -> Self {
        Self(cache, shard_id)
    }

    pub fn update(&self, value: impl UpdateCache) {
        self.0.update(value);
    }

    pub fn stats(&self) -> InMemoryCacheStats {
        self.0.stats()
    }

    pub fn get_ready_payload(&self, mut ready: JsonObject, sequence: &mut usize) -> Payload {
        *sequence += 1;

        let unavailable_guilds = self
            .0
            .iter()
            .guilds()
            .map(|guild| {
                #[cfg(feature = "simd-json")]
                {
                    hashmap! {
                        String::from("id") => guild.id().to_string().into(),
                        String::from("unavailable") => true.into(),
                    }
                    .into()
                }
                #[cfg(not(feature = "simd-json"))]
                {
                    serde_json::json!({
                        "id": guild.id().to_string(),
                        "unavailable": true
                    })
                }
            })
            .collect();

        ready.insert(
            String::from("guilds"),
            OwnedValue::Array(unavailable_guilds),
        );

        Payload {
            d: Event::Ready(ready),
            op: OpCode::Dispatch,
            t: String::from("READY"),
            s: *sequence,
        }
    }

    fn channels_in_guild(&self, guild_id: Id<GuildMarker>) -> Vec<Channel> {
        self.0
            .guild_channels(guild_id)
            .map(|reference| {
                reference
                    .iter()
                    .filter_map(|channel_id| {
                        let channel = self.0.channel(*channel_id)?;

                        if channel.kind.is_thread() {
                            None
                        } else {
                            Some(channel.value().clone())
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn member(&self, guild_id: Id<GuildMarker>, user_id: Id<UserMarker>) -> Option<Member> {
        let member = self.0.member(guild_id, user_id)?;

        Some(Member {
            avatar: member.avatar(),
            communication_disabled_until: member.communication_disabled_until(),
            deaf: member.deaf().unwrap_or_default(),
            flags: member.flags(),
            joined_at: member.joined_at(),
            mute: member.mute().unwrap_or_default(),
            nick: member.nick().map(ToString::to_string),
            pending: member.pending(),
            premium_since: member.premium_since(),
            roles: member.roles().to_vec(),
            user: self.0.user(member.user_id())?.value().clone(),
        })
    }

    fn members_in_guild(&self, guild_id: Id<GuildMarker>) -> Vec<Member> {
        self.0
            .guild_members(guild_id)
            .map(|reference| {
                reference
                    .iter()
                    .filter_map(|user_id| self.member(guild_id, *user_id))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn roles_in_guild(&self, guild_id: Id<GuildMarker>) -> Vec<Role> {
        self.0
            .guild_roles(guild_id)
            .map(|reference| {
                reference
                    .iter()
                    .filter_map(|role_id| Some(self.0.role(*role_id)?.value().resource().clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn voice_states_in_guild(&self, guild_id: Id<GuildMarker>) -> Vec<VoiceState> {
        self.0
            .guild_voice_states(guild_id)
            .map(|reference| {
                reference
                    .iter()
                    .filter_map(|user_id| {
                        let voice_state = self.0.voice_state(*user_id, guild_id)?;

                        Some(VoiceState {
                            channel_id: Some(voice_state.channel_id()),
                            deaf: voice_state.deaf(),
                            guild_id: Some(voice_state.guild_id()),
                            member: self.member(guild_id, *user_id),
                            mute: voice_state.mute(),
                            self_deaf: voice_state.self_deaf(),
                            self_mute: voice_state.self_mute(),
                            self_stream: voice_state.self_stream(),
                            self_video: voice_state.self_video(),
                            session_id: voice_state.session_id().to_string(),
                            suppress: voice_state.suppress(),
                            user_id: voice_state.user_id(),
                            request_to_speak_timestamp: voice_state.request_to_speak_timestamp(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn threads_in_guild(&self, guild_id: Id<GuildMarker>) -> Vec<Channel> {
        self.0
            .guild_channels(guild_id)
            .map(|reference| {
                reference
                    .iter()
                    .filter_map(|channel_id| {
                        let channel = self.0.channel(*channel_id)?;

                        if channel.kind.is_thread() {
                            Some(channel.value().clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_guild_payloads<'a>(
        &'a self,
        sequence: &'a mut usize,
    ) -> impl Iterator<Item = Payload> + 'a {
        self.0.iter().guilds().map(move |guild| {
            *sequence += 1;

            if guild.unavailable() {
                let guild_delete = GuildDelete {
                    id: guild.id(),
                    unavailable: true,
                };

                Payload {
                    d: Event::GuildDelete(guild_delete),
                    op: OpCode::Dispatch,
                    t: String::from("GUILD_DELETE"),
                    s: *sequence,
                }
            } else {
                let guild_channels = self.channels_in_guild(guild.id());
                let members = self.members_in_guild(guild.id());
                let roles = self.roles_in_guild(guild.id());
                let voice_states = self.voice_states_in_guild(guild.id());
                let threads = self.threads_in_guild(guild.id());

                let new_guild = Guild {
                    channels: guild_channels,
                    id: guild.id(),
                    member_count: guild.member_count(),
                    members,
                    name: guild.name().to_string(),
                    owner_id: guild.owner_id(),
                    permissions: guild.permissions(),
                    roles,
                    threads,
                    unavailable: false,
                    voice_states,
                };

                let guild_create = GuildCreate(new_guild);

                Payload {
                    d: Event::GuildCreate(Box::new(guild_create)),
                    op: OpCode::Dispatch,
                    t: String::from("GUILD_CREATE"),
                    s: *sequence,
                }
            }
        })
    }
}
