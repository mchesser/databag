use std::iter::FromIterator;

use query::Queryable;

pub struct FactorData<T> {
    pub rows: Vec<usize>,
    pub factors: Vec<T>
}

impl<T> FactorData<T> {
    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

impl FactorData<String> {
    pub fn new<'a, I>(factors: Vec<String>, values: I) -> FactorData<String>
    where
        I : Iterator<Item=&'a str>,
    {
        let mut data = FactorData { rows: vec![], factors: factors };
        for value in values {
            data.add_row(value)
        }
        data
    }

    pub fn add_row(&mut self, value: &str) {
        match self.factors.iter().position(|x| x == value) {
            Some(i) => self.rows.push(i),
            None => {
                self.rows.push(self.factors.len());
                self.factors.push(value.into());
            }
        }
    }
}

impl<T: Clone + PartialEq> Queryable for FactorData<T> {
    type Item = T;

    fn select<'a, I, F>(&'a self, indices: I, mut func: F)
    where
        I: Iterator<Item=usize>,
        F: FnMut(usize, &'a Self::Item)
    {
        for i in indices {
            (func)(i, &self.factors[self.rows[i]]);
        }
    }

    fn apply<I, F>(&mut self, indices: I, mut func: F)
    where
        I: Iterator<Item=usize>,
        F: FnMut(usize, &Self::Item) -> Self::Item
    {
        for i in indices {
            let value = (func)(i, &self.factors[self.rows[i]]);
            self.rows[i] = self.factors.iter().position(|f| f == &value)
                .expect("Factor does not exist");
        }
    }
}

impl<'a> FromIterator<&'a str> for FactorData<String> {
    fn from_iter<T: IntoIterator<Item=&'a str>>(iter: T) -> FactorData<String> {
        FactorData::new(vec![], iter.into_iter())
    }
}
