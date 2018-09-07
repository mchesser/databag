use std::any::{Any, TypeId};
use std::collections::HashMap;

use column::Column;

pub struct DataFrame {
    columns: HashMap<String, Entry>,
}

struct Entry {
    value: Box<dyn Any + 'static>,
    type_id: TypeId,
}

impl Entry {
    fn new<T: 'static, C: Into<Column<T>>>(col: C) -> Entry {
        Entry {
            value: Box::new(col.into()) as Box<dyn Any>,
            type_id: TypeId::of::<T>(),
        }
    }
}

impl DataFrame {
    pub fn new() -> DataFrame {
        DataFrame {
            columns: HashMap::new(),
        }
    }

    pub fn add_column<T: 'static, C: Into<Column<T>>>(&mut self, name: String, col: C) {
        self.columns.insert(name, Entry::new(col));
    }

    pub fn get<T: 'static>(&self, field: &str) -> &Column<T> {
        self.columns[field].value.downcast_ref().unwrap()
    }

    pub fn get_mut<T: 'static>(&mut self, field: &str) -> &mut Column<T> {
        self.columns.get_mut(field).unwrap().value.downcast_mut().unwrap()
    }

    pub fn is_type<T: 'static>(&self, field: &str) -> bool {
        self.columns[field].type_id == TypeId::of::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn get_values() {
        let mut data = DataFrame::new();
        data.add_column("nums".into(), vec![0, 1]);

        data.add_column("factors".into(),
            Column::factor(["apples", "apples", "bananas"].iter().map(|&x| x).collect())
        );

        assert!(data.is_type::<i32>("nums"));
        assert!(data.is_type::<String>("factors"));

        assert_eq!(data.get::<i32>("nums")[0], 0);
        assert_eq!(data.get::<i32>("nums")[1], 1);

        assert_eq!(data.get::<String>("factors")[0], "apples");
        assert_eq!(data.get::<String>("factors")[1], "apples");
        assert_eq!(data.get::<String>("factors")[2], "bananas");
    }

    #[test]
    fn set_values() {
        let mut data = DataFrame::new();
        data.add_column("nums".into(), vec![0; 2]);

        data.add_column("factors".into(),
            Column::factor(["apples", "apples", "bananas"].iter().map(|&x| x).collect())
        );

        data.get_mut::<i32>("nums").set(0, 10);
        data.get_mut::<i32>("nums").set(1, 20);

        assert_eq!(data.get::<i32>("nums")[0], 10);
        assert_eq!(data.get::<i32>("nums")[1], 20);

        data.get_mut::<String>("factors").set(0, "bananas".into());
        data.get_mut::<String>("factors").set(1, "bananas".into());
        data.get_mut::<String>("factors").set(2, "apples".into());

        assert_eq!(data.get::<String>("factors")[0], "bananas");
        assert_eq!(data.get::<String>("factors")[1], "bananas");
        assert_eq!(data.get::<String>("factors")[2], "apples");
    }
}