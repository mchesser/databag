pub trait Queryable {
    type Item: Clone;

    /// Iterates over the specified indices of the column
    fn select<'a, I, F>(&'a self, indices: I, func: F)
    where
        I: Iterator<Item=usize>,
        F: FnMut(usize, &'a Self::Item);

    /// Sets a specific value of a function
    fn apply<I, F>(&mut self, indices: I, func: F)
    where
        I: Iterator<Item=usize>,
        F: FnMut(usize, &Self::Item) -> Self::Item;

    /// Clones the selected rows of the column to an output vector
    fn clone_to<I: Iterator<Item=usize>>(&self, index: I, out: &mut Vec<Self::Item>) {
        out.reserve(index.size_hint().0);
        self.select(index, |_, x| out.push(x.clone()));
    }

    /// Creates a new vector from the selected rows of the column
    fn to_vec<I: Iterator<Item=usize>>(&self, index: I) -> Vec<Self::Item> {
        let mut out = Vec::new();
        self.clone_to(index, &mut out);
        out
    }
}

impl<T: Clone> Queryable for Vec<T> {
    type Item = T;

    fn select<'a, I, F>(&'a self, indices: I, mut func: F)
    where
        I: Iterator<Item=usize>,
        F: FnMut(usize, &'a Self::Item)
    {
        for i in indices {
            (func)(i, &self[i]);
        }
    }

    fn apply<I, F>(&mut self, indices: I, mut func: F)
    where
        I: Iterator<Item=usize>,
        F: FnMut(usize, &Self::Item) -> Self::Item
    {
        for i in indices {
            let value = { (func)(i, &self[i]) };
            self[i] = value;
        }
    }
}
