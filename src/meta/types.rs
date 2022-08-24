use super::inode::Inode;
use crate::schema_capnp;
use anyhow::Result;
use capnp::{message, serialize};
use nix::unistd::{Group, User};
use std::convert::TryInto;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Node {
    pub inode: Inode,
    pub name: String,
    pub size: u64,
    pub acl: String,
    pub modification: u32,
    pub creation: u32,
}

#[derive(Debug, Clone)]
pub struct SubDir {
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct FileBlock {
    pub hash: [u8; 16],
    pub key: [u8; 16],
}

#[derive(Debug, Clone)]
pub struct File {
    pub block_size: u16,
    pub blocks: Vec<FileBlock>,
}

#[derive(Debug, Clone)]
pub struct Link {
    pub target: String,
}

#[derive(Debug, Clone)]
pub struct Dir {
    pub key: String,
    pub parent: String,
    // we use arch for shallow clone of directory
    pub entries: Arc<Vec<Entry>>,
}

impl Dir {
    pub fn from<S: AsRef<str>>(key: S, inode: Inode, data: Vec<u8>) -> Result<Entry> {
        let mut raw: &[u8] = data.as_ref();

        let msg = serialize::read_message(&mut raw, message::ReaderOptions::default())?;

        let root = msg.get_root::<schema_capnp::dir::Reader>()?;
        let name: String = root.get_name()?.into();
        let parent: String = root.get_parent()?.into();
        let size = root.get_size();
        let modification = root.get_modification_time();
        let creation = root.get_creation_time();
        let entries = Dir::entries(inode, root)?;

        Ok(Entry {
            node: Node {
                inode,
                name,
                size,
                acl: "".into(),
                modification,
                creation,
            },
            kind: EntryKind::Dir(Dir {
                parent,
                key: key.as_ref().into(),
                entries: Arc::new(entries),
            }),
        })
    }

    fn entries(ino: Inode, dir: schema_capnp::dir::Reader) -> Result<Vec<Entry>> {
        /*
        This definitely needs refactoring
        */
        use schema_capnp::inode::attributes::Which;

        let mut entries: Vec<Entry> = vec![];
        let mut x = 0;

        for entry in dir.get_contents()? {
            x += 1;
            let inode = ino.at(x);
            let node = Node {
                inode,
                //parent: inode,
                name: String::from(entry.get_name()?),
                size: entry.get_size(),
                acl: String::from(entry.get_aclkey()?),
                modification: entry.get_modification_time(),
                creation: entry.get_creation_time(),
            };

            let attrs = entry.get_attributes();
            let kind = match attrs.which()? {
                Which::Dir(d) => {
                    let key = String::from(d?.get_key()?);
                    EntryKind::SubDir(SubDir { key })
                }
                Which::File(f) => {
                    let f = f?;

                    EntryKind::File(File {
                        block_size: f.get_block_size(),
                        blocks: match f.get_blocks() {
                            Ok(blocks) => {
                                let mut result = vec![];
                                for block in blocks {
                                    result.push(FileBlock {
                                        hash: block
                                            .get_hash()?
                                            .try_into()
                                            .expect("block hash is 16 bytes"),
                                        key: block
                                            .get_key()?
                                            .try_into()
                                            .expect("block encryption key is 16 bytes"),
                                    });
                                }
                                result
                            }
                            Err(err) => return Err(anyhow!(err)),
                        },
                    })
                }
                Which::Link(l) => {
                    let l = l?;
                    EntryKind::Link(Link {
                        target: String::from(l.get_target()?),
                    })
                }
                _ => EntryKind::Unknown,
            };

            if let EntryKind::Unknown = kind {
                continue;
            }

            entries.push(Entry { node, kind });
        }

        Ok(entries)
    }
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub node: Node,
    pub kind: EntryKind,
}

#[derive(Debug, Clone)]
pub enum EntryKind {
    Unknown,
    Dir(Dir),
    SubDir(SubDir),
    File(File),
    Link(Link),
}

#[derive(Clone)]
pub struct Aci {
    pub user: u32,
    pub group: u32,
    pub mode: u32,
}

impl Aci {
    pub fn new(data: Vec<u8>) -> Result<Aci> {
        let mut raw: &[u8] = data.as_ref();
        let msg = serialize::read_message(&mut raw, message::ReaderOptions::default())?;

        let root = msg.get_root::<schema_capnp::a_c_i::Reader>()?;
        let mut uid = root.get_uid();
        let mut gid = root.get_gid();
        let mode = root.get_mode();

        if uid == -1 {
            // backward compatibility with older flist
            uid = if root.has_uname() {
                let uname = root.get_uname().unwrap();
                match User::from_name(uname) {
                    Ok(Some(user)) => user.uid.as_raw() as i64,
                    _ => 1000,
                }
            } else {
                1000
            };
        }

        if gid == -1 {
            // backward compatibility with older flist
            gid = if root.has_gname() {
                let gname = root.get_gname().unwrap();
                match Group::from_name(gname) {
                    Ok(Some(group)) => group.gid.as_raw() as i64,
                    _ => 1000,
                }
            } else {
                1000
            };
        }

        Ok(Aci {
            user: uid as u32,
            group: gid as u32,
            mode: mode as u32,
        })
    }
}
