/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;

use keep_proto::{NodeId, Nonce, PEER_HELLO_LEN, PeerSecret, PeerSide};

use crate::handoff::frames::{PeerFrame, PeerHello};
use crate::membership::Membership;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PeerLink {
    pub node: NodeId,
    pub index: usize,
    pub incarnation: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PeerError {
    Unexpected,
    NotForMe(NodeId),
    NotAMember(NodeId),
    WrongPeer(NodeId),
    MemberList,
    Proof,
}

impl fmt::Display for PeerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerError::Unexpected => f.write_str("unexpected frame in the peer handshake"),
            PeerError::NotForMe(node) => write!(f, "peer hello addressed to node {}", node.0),
            PeerError::NotAMember(node) => write!(f, "peer hello from non-member {}", node.0),
            PeerError::WrongPeer(node) => write!(f, "peer hello from node {}, another one", node.0),
            PeerError::MemberList => f.write_str("peer hello from another member list"),
            PeerError::Proof => f.write_str("peer proof failed"),
        }
    }
}

impl std::error::Error for PeerError {}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Step {
    Hello,
    Proof,
    Done,
}

#[derive(Clone, Debug)]
pub struct Handshake {
    side: PeerSide,
    mine: PeerHello,
    theirs: Option<PeerHello>,
    step: Step,
}

impl Handshake {
    pub fn initiate(membership: &Membership, to: NodeId, nonce: Nonce) -> (Handshake, PeerHello) {
        let mine = hello(membership, to, nonce);
        let handshake = Handshake {
            side: PeerSide::Initiator,
            mine,
            theirs: None,
            step: Step::Hello,
        };
        (handshake, mine)
    }

    pub fn accept(membership: &Membership, nonce: Nonce) -> Handshake {
        Handshake {
            side: PeerSide::Acceptor,
            mine: hello(membership, NodeId(0), nonce),
            theirs: None,
            step: Step::Hello,
        }
    }

    pub fn step(
        &mut self,
        frame: &PeerFrame<'_>,
        secret: &PeerSecret,
        membership: &Membership,
        out: &mut Vec<u8>,
    ) -> Result<Option<PeerLink>, PeerError> {
        match (self.step, self.side, frame) {
            (Step::Hello, side, PeerFrame::Hello(theirs)) => {
                self.check(theirs, membership)?;
                self.theirs = Some(*theirs);
                self.step = Step::Proof;
                if side == PeerSide::Acceptor {
                    self.mine.receiver = theirs.sender;
                    PeerFrame::Hello(self.mine).encode(out);
                    let (initiator, acceptor) = self.transcript(theirs);
                    let proof = secret.transcript_proof(PeerSide::Acceptor, &initiator, &acceptor);
                    PeerFrame::Proof(proof.0).encode(out);
                }
                Ok(None)
            }
            (Step::Proof, side, PeerFrame::Proof(proof)) => {
                let theirs = self.theirs.ok_or(PeerError::Unexpected)?;
                let (initiator, acceptor) = self.transcript(&theirs);
                let other = match side {
                    PeerSide::Initiator => PeerSide::Acceptor,
                    PeerSide::Acceptor => PeerSide::Initiator,
                };
                if !secret.verify_transcript(other, &initiator, &acceptor, proof) {
                    return Err(PeerError::Proof);
                }
                if side == PeerSide::Initiator {
                    let proof = secret.transcript_proof(PeerSide::Initiator, &initiator, &acceptor);
                    PeerFrame::Proof(proof.0).encode(out);
                }
                self.step = Step::Done;
                let index = membership
                    .index_of(theirs.sender)
                    .ok_or(PeerError::NotAMember(theirs.sender))?;
                Ok(Some(PeerLink {
                    node: theirs.sender,
                    index,
                    incarnation: theirs.incarnation,
                }))
            }
            _ => Err(PeerError::Unexpected),
        }
    }

    fn check(&self, theirs: &PeerHello, membership: &Membership) -> Result<(), PeerError> {
        if theirs.receiver != membership.node() {
            return Err(PeerError::NotForMe(theirs.receiver));
        }
        if theirs.sender == membership.node() || membership.index_of(theirs.sender).is_none() {
            return Err(PeerError::NotAMember(theirs.sender));
        }
        if self.side == PeerSide::Initiator && theirs.sender != self.mine.receiver {
            return Err(PeerError::WrongPeer(theirs.sender));
        }
        if theirs.member_list != membership.member_list() {
            return Err(PeerError::MemberList);
        }
        Ok(())
    }

    fn transcript(&self, theirs: &PeerHello) -> ([u8; PEER_HELLO_LEN], [u8; PEER_HELLO_LEN]) {
        match self.side {
            PeerSide::Initiator => (self.mine.to_bytes(), theirs.to_bytes()),
            PeerSide::Acceptor => (theirs.to_bytes(), self.mine.to_bytes()),
        }
    }
}

fn hello(membership: &Membership, receiver: NodeId, nonce: Nonce) -> PeerHello {
    PeerHello {
        sender: membership.node(),
        receiver,
        incarnation: membership.incarnation(),
        member_list: membership.member_list(),
        nonce,
    }
}
