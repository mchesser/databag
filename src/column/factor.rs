use std::iter::FromIterator;

use query::Queryable;

pub struct FactorData<T> {
    rows: Vec<usize>,
    factors: Vec<T>
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
        let mut data = FactorData { rows: vec![], factors: vec![] };
        for value in iter {
            match data.factors.iter().position(|x| x == value) {
                Some(i) => data.rows.push(i),
                None => {
                    data.rows.push(data.factors.len());
                    data.factors.push(value.into());
                }
            }
        }

        data
    }
}
