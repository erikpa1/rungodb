use std::collections::HashMap;

use std::sync::{Arc, Mutex};
use std::vec;
use uuid::Uuid;

use serde_json::{json, Value};

use serde_json::map::Map as SerdeMap;

pub struct RungoDB {
    pub jsondb: Value,
    pub single_file: bool,
}

impl RungoDB {
    pub fn New() -> Self {
        return Self {
            jsondb: json!({}),
            single_file: true,
        };
    }

    pub fn GetOrCreateContainer(&mut self, name: &String) -> Option<&mut SerdeMap<String, Value>> {
        if let Some(ct) = self.jsondb.as_object_mut() {
            if ct.contains_key(name) {
            } else {
                let mut tmp = SerdeMap::new();
                ct.insert(name.clone(), Value::from(tmp));
            }
            return ct.get_mut(name).map(|v| v.as_object_mut().unwrap());
        }

        None
    }

    pub fn InsertEntity(&mut self, container: &String, mut entity: Value) -> Result<String, ()> {
        let mut uid = String::from("");
        let mut return_uid = String::from("");

        if let Some(val_obj) = entity.as_object_mut() {
            if let Some(val) = val_obj.get("uid") {
                if let Some(uid_val) = val.as_str() {
                    if uid_val == "" {
                        uid = Uuid::new_v4().to_string();
                        val_obj.insert("uid".into(), Value::String(uid.clone()));
                    } else {
                        uid = String::from(uid_val);
                    }
                }
            } else {
                uid = Uuid::new_v4().to_string();
                val_obj.insert("uid".into(), Value::String(uid.clone()));
            }
        }

        return_uid = uid.clone();

        let container_r = self.GetOrCreateContainer(container);

        if let Some(map) = container_r {
            map.insert(uid, entity);
        }

        Ok(return_uid)
    }

    pub fn DeleteEntities(&mut self, container_key: &String, query: &Value) {
        let container_r = self.GetOrCreateContainer(container_key);

        if let Some(container) = container_r {
            if let Some(query_object) = query.as_object() {
                let keys_len = query_object.keys().len();

                if keys_len == 0 {
                    container.clear();
                } else if (keys_len == 1 && query_object.contains_key("uid")) {
                    let mut keys_to_delete: Vec<String> = vec![];

                    for val in container.values() {
                        let uid = String::from(val.get("uid").unwrap().as_str().unwrap());
                        keys_to_delete.push(uid);
                    }

                    for key in keys_to_delete {
                        container.remove_entry(&key);
                    }
                } else {
                    let mut keys_to_delete: Vec<String> = vec![];

                    for val in container.values() {
                        if Self::_ValueMatchQuery(val, query_object) {
                            let uid = String::from(val.get("uid").unwrap().as_str().unwrap());
                            keys_to_delete.push(uid);
                        }
                    }

                    for key in keys_to_delete {
                        container.remove_entry(&key);
                    }
                }
            }
        }
    }

    pub fn QueryEntities(&mut self, container_key: &String, query: &Value) -> Vec<Value> {
        let mut result: Vec<Value> = vec![];

        let container_r = self.GetOrCreateContainer(container_key);

        if let Some(container) = container_r {
            if let Some(query_object) = query.as_object() {
                let keys_len = query_object.keys().len();

                if keys_len == 0 {
                    return container.values().map(|val| val.clone()).collect();
                } else if (keys_len == 1 && query_object.contains_key("uid")) {
                    if let Some(uid_val_jobj) = query_object.get("uid") {
                        if let Some(uid_val) = uid_val_jobj.as_str() {
                            if let Some(entity) = container.get(&String::from(uid_val)) {
                                return vec![entity.clone()];
                            }
                        }
                    }
                } else {
                    return container
                        .values()
                        .filter(|&val| Self::_ValueMatchQuery(val, query_object))
                        .map(|val| val.clone())
                        .collect();
                }
            }
        }

        return result;
    }

    fn _ValueMatchQuery(val: &Value, query: &serde_json::Map<String, Value>) -> bool {
        if let Some(query_object) = val.as_object() {
            for i in query_object.keys() {
                if let Some(val) = query.get(i) {
                    let result = val.eq(val);
                    return result;
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    pub fn ToJson(&self) -> Value {
        // json!({
        //     "name": "Some name",
        //     "containers": self.containers
        // })
        return self.jsondb.clone();
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::RungoDB;

    #[test]
    fn it_works() {
        let mut db = RungoDB::New();

        db.InsertEntity(
            &"projects".into(),
            json!({"name": "My project", "uid":"xxya"}),
        );

        let result = db.QueryEntities(&"projects".into(), &json!({"uid": "xxya"}));

        assert_eq!(result.len(), 1);
    }
}
