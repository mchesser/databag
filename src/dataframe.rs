use std::any::Any;
use std::collections::HashMap;

use column::Column;

pub struct DataFrame {
    columns: HashMap<String, Box<Any + 'static>>
}

impl DataFrame {
    pub fn new() -> DataFrame {
        DataFrame {
            columns: HashMap::new(),
        }
    }

    pub fn add_column<T: 'static, C: Into<Column<T>>>(&mut self, name: String, col: C) {
        self.columns.insert(name, Box::new(col.into()) as Box<Any>);
    }

    pub fn get<T: 'static>(&self, field: &str) -> &Column<T> {
        self.columns[field].downcast_ref().unwrap()
    }

    pub fn get_mut<T: 'static>(&mut self, field: &str) -> &mut Column<T> {
        self.columns.get_mut(field).unwrap().downcast_mut().unwrap()
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