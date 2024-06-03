//! [`BuilderArgs`] trait definition.

use lapin::types::{
    AMQPValue, Boolean, Double, FieldTable, Float, LongInt, LongLongInt, LongString, LongUInt,
    ShortInt, ShortShortInt, ShortShortUInt, ShortString, ShortUInt,
};

/// Different functions to set key value pairs on [`FieldTable`]s.
pub trait BuilderArgs: Sized {
    /// Get the [`FieldTable`] for a set of arguments.
    fn get_args(&mut self) -> &mut FieldTable;

    /// Set an argument on the [`FieldTable`].
    #[must_use]
    fn arg<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<AMQPValue>,
    {
        self.get_args().insert(k.into(), v.into());
        self
    }

    /// Set a boolean argument.
    #[must_use]
    fn arg_bool<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<Boolean>,
    {
        self.get_args()
            .insert(k.into(), AMQPValue::Boolean(v.into()));
        self
    }

    /// Set a `string` argument.
    #[must_use]
    fn arg_str<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<LongString>,
    {
        self.get_args()
            .insert(k.into(), AMQPValue::LongString(v.into()));
        self
    }

    /// Set an `u8` argument.
    #[must_use]
    fn arg_u8<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<ShortShortUInt>,
    {
        self.get_args()
            .insert(k.into(), AMQPValue::ShortShortUInt(v.into()));
        self
    }

    /// Set an `u16` argument.
    #[must_use]
    fn arg_u16<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<ShortUInt>,
    {
        self.get_args()
            .insert(k.into(), AMQPValue::ShortUInt(v.into()));
        self
    }

    /// Set an `u32` argument.
    #[must_use]
    fn arg_u32<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<LongUInt>,
    {
        self.get_args()
            .insert(k.into(), AMQPValue::LongUInt(v.into()));
        self
    }

    /// Set an `i8` argument.
    #[must_use]
    fn arg_i8<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<ShortShortInt>,
    {
        self.get_args()
            .insert(k.into(), AMQPValue::ShortShortInt(v.into()));
        self
    }

    /// Set an `i16` argument.
    #[must_use]
    fn arg_i16<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<ShortInt>,
    {
        self.get_args()
            .insert(k.into(), AMQPValue::ShortInt(v.into()));
        self
    }

    /// Set an `i32` argument.
    #[must_use]
    fn arg_i32<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<LongInt>,
    {
        self.get_args()
            .insert(k.into(), AMQPValue::LongInt(v.into()));
        self
    }

    /// Set an `i64` argument.
    #[must_use]
    fn arg_i64<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<LongLongInt>,
    {
        self.get_args()
            .insert(k.into(), AMQPValue::LongLongInt(v.into()));
        self
    }

    /// Set an `f32` argument.
    #[must_use]
    fn arg_f32<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<Float>,
    {
        self.get_args().insert(k.into(), AMQPValue::Float(v.into()));
        self
    }

    /// Set an `f64` argument.
    #[must_use]
    fn arg_f64<K, V>(mut self, k: K, v: V) -> Self
    where
        K: Into<ShortString>,
        V: Into<Double>,
    {
        self.get_args()
            .insert(k.into(), AMQPValue::Double(v.into()));
        self
    }

    /// Args
    #[must_use]
    fn args<F>(mut self, f: F) -> Self
    where
        F: FnOnce(FieldTable) -> FieldTable,
    {
        let args = self.get_args();
        *args = f(args.clone());
        self
    }
}
