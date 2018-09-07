use std::any::{Any, TypeId};
use std::collections::HashMap;

use column::{Column, FactorData};

pub struct DataFrame {
    columns: HashMap<String, Entry>,
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

    pub fn get_dynamic(&self, field: &str) -> DynamicField {
        self.columns[field].get_dynamic()
    }

    pub fn get_mut<T: 'static>(&mut self, field: &str) -> &mut Column<T> {
        self.columns.get_mut(field).unwrap().value.downcast_mut().unwrap()
    }

    pub fn is_type<T: 'static>(&self, field: &str) -> bool {
        self.columns[field].value.is::<Column<T>>()
    }
}


struct Entry {
    value: Box<dyn Any + 'static>,
}

impl Entry {
    fn new<T: 'static, C: Into<Column<T>>>(col: C) -> Entry {
        Entry {
            value: Box::new(col.into()) as Box<dyn Any>,
        }
    }

    fn get_dynamic(&self) -> DynamicField {
        if self.value.is::<Column<i64>>() {
            DynamicField::Int64(self.value.downcast_ref::<Column<i64>>().unwrap().as_ref())
        }
        else if self.value.is::<Column<f32>>() {
            DynamicField::Float32(self.value.downcast_ref::<Column<f32>>().unwrap().as_ref())
        }
        else if self.value.is::<Column<f64>>() {
            DynamicField::Float64(self.value.downcast_ref::<Column<f64>>().unwrap().as_ref())
        }
        else if self.value.is::<Column<String>>() {
            DynamicField::String(self.value.downcast_ref::<Column<String>>().unwrap().as_factor())
        }
        else {
            // TODO tighten constraints on T so this can't happen
            unimplemented!()
        }
    }

    fn get_dynamic_mut(&mut self) -> DynamicFieldMut {
        unimplemented!()
    }
}

pub enum DynamicField<'a> {
    Int64(&'a [i64]),
    Float32(&'a [f32]),
    Float64(&'a [f64]),
    String(&'a FactorData<String>),
}

pub enum DynamicFieldMut<'a> {
    Int64(&'a mut [i64]),
    Float32(&'a mut [f32]),
    Float64(&'a mut [f64]),
    String(&'a mut FactorData<String>),
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

    #[test]
    fn dynamic() {
        let mut data = DataFrame::new();
        data.add_column("ints".into(), vec![0_i64; 2]);
        data.add_column("floats".into(), vec![0.0_f32; 2]);
        data.add_column("factors".into(),
            Column::factor(["apples", "apples", "bananas"].iter().map(|&x| x).collect())
        );

        match data.get_dynamic("ints") {
            DynamicField::Int64(_ints) => {},
            _ => panic!("Incorrect dynamic type"),
        }

        match data.get_dynamic("floats") {
            DynamicField::Float32(_floats) => {},
            _ => panic!("Incorrect dynamic type"),
        }

        match data.get_dynamic("factors") {
            DynamicField::String(_factors) => {},
            _ => panic!("Incorrect dynamic type"),
        }
    }
}