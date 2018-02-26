use std::any::Any;

pub struct Column {
    name: String,
    rows: Box<Any>,
}

pub struct DataFrame {
    columns: Vec<Column>
}

impl DataFrame {
    pub fn new() -> DataFrame {
        DataFrame {
            columns: vec![]
        }
    }

    pub fn add_column<T: 'static>(&mut self, name: String, rows: Vec<T>) {
        let rows = Box::new(rows) as Box<Any>;
        self.columns.push(Column { name, rows });
    }

    pub fn get<T: 'static>(&self, id: usize) -> &[T] {
        let rows = self.columns[id].rows.downcast_ref::<Vec<T>>().unwrap();
        &rows
    }

    pub fn get_mut<T: 'static>(&mut self, id: usize) -> &mut [T] {
        let rows: &mut Vec<T> = self.columns[id].rows.downcast_mut::<Vec<T>>().unwrap();
        rows.as_mut_slice()
    }

    pub fn index_of(&self, field: &str) -> Option<usize> {
        self.columns.iter().position(|x| x.name == field)
    }

    pub fn select<T: 'static>(&self, field: &str) -> &[T] {
        self.get(self.index_of(field).unwrap())
    }

    pub fn select_mut<T: 'static>(&mut self, field: &str) -> &mut [T] {
        let index = self.index_of(field).unwrap();
        self.get_mut(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_values() {
        let mut data = DataFrame::new();
        data.add_column("test".into(), vec![0, 1]);

        assert_eq!(data.get::<i32>(0)[0], 0);
        assert_eq!(data.get::<i32>(0)[1], 1);

        assert_eq!(data.select::<i32>("test")[0], 0);
        assert_eq!(data.select::<i32>("test")[1], 1);
    }

    #[test]
    fn set_values() {
        let mut data = DataFrame::new();
        data.add_column("test".into(), vec![0; 2]);

        data.get_mut(0)[0] = 100;
        data.get_mut(0)[1] = 200;

        assert_eq!(data.get::<i32>(0)[0], 100);
        assert_eq!(data.get::<i32>(0)[1], 200);

        data.select_mut::<i32>("test")[0] = 10;
        data.select_mut::<i32>("test")[1] = 20;

        assert_eq!(data.select::<i32>("test")[0], 10);
        assert_eq!(data.select::<i32>("test")[1], 20);
    }
}
