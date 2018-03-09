use std::iter::FromIterator;
use std::ops::Index;

use query::Queryable;

/// Represents a data type that can be used as a dataframe column
///
/// # Panics
///
/// Most methods will panic if the inner type of column cannot be interpreted as the target output
/// type
pub enum Column<T> {
    Vec(Vec<T>),
    Factor(FactorData<T>),
}

impl<T: Clone + PartialEq> Column<T> {
    /// Creates a new column with factor data
    pub fn factor(data: FactorData<T>) -> Column<T> {
        Column::Factor(data)
    }

    /// Returns a reference to the complete data in the column
    pub fn as_ref(&self) -> &[T] {
        match *self {
            Column::Vec(ref rows) => rows,
            Column::Factor(_) => panic!("Can't get a factor field as a slice"),
        }
    }

    /// Returns a mutable reference to the complete data in the column
    pub fn as_mut(&mut self) -> &mut [T] {
        match *self {
            Column::Vec(ref mut rows) => rows,
            Column::Factor(_) => panic!("Can't get a factor field as a slice"),
        }
    }

    /// Returns a reference to an element in the column
    pub fn get(&self, index: usize) -> Option<&T> {
        let mut result = None;
        self.select(Some(index).into_iter(), |_, x| result = Some(x));
        result
    }

    /// Sets the value of a single element in the column
    pub fn set(&mut self, index: usize, value: T) {
        let mut value = Some(value);
        self.apply(Some(index).into_iter(), |_, _| value.take().unwrap());
    }

    pub fn as_factor(&self) -> &FactorData<T> {
        match *self {
            Column::Vec(_) => panic!("Column is not a factor field"),
            Column::Factor(ref data) => data,
        }
    }
}

impl<T: Clone + PartialEq> Queryable for Column<T> {
    type Item = T;

    fn select<'a, I, F>(&'a self, indices: I, func: F)
    where
        I: Iterator<Item=usize>,
        F: FnMut(usize, &'a Self::Item)
    {
        match *self {
            Column::Vec(ref x) => x.select(indices, func),
            Column::Factor(ref x) => x.select(indices, func),
        }
    }

    fn apply<I, F>(&mut self, indices: I, func: F)
    where
        I: Iterator<Item=usize>,
        F: FnMut(usize, &Self::Item) -> Self::Item
    {
        match *self {
            Column::Vec(ref mut x) => x.apply(indices, func),
            Column::Factor(ref mut x) => x.apply(indices, func),
        }
    }
}

impl<T: Clone + PartialEq> Index<usize> for Column<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        self.get(index).unwrap()
    }
}

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

impl<T> From<Vec<T>> for Column<T> {
    fn from(value: Vec<T>) -> Column<T> {
        Column::Vec(value)
    }
}
