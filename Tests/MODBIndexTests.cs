namespace MODB.Tests;

public class MODBIndexTests
{
    [Fact]
    public void Filter_WhenCountEqualZeroFilterItemsEmpty_WhenCountGreaterThanZeroFilterItemsCountEqualNumberOfMatchesInSets_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "all_test_db"));
        db.Set("blablalablaba", "testvalue"); db.Clear(); // initialize key index 

        Assert.Equal(0, db.Count());
        Assert.Empty(db.All().Items);

        var numberOfSets = 10;
        var values = Enumerable.Range(1, numberOfSets).Select(i => {
            var value = $"testvalue{i}";
            db.Set(i.ToString(), value);
            return value;
        }).ToArray();

        Assert.True(values.Length > 0 && values.Length == db.Count());
        var all = db.All(pageSize: numberOfSets);
        Assert.Equal(db.Count(), all.Items.Count());

        for(int i = 0; i < values.Length; i ++){
            Assert.Contains(values[i], all.Items);
        }

        db.Clear();
    }

    [Fact]
    public void First_WhenExistsEqualFalseThenFirstThrowsKeyNotFoundExceptionElseReturnSetValue_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "first_test_db"));
        var key = "testkey";
        var value = "testvalue";
        db.Set("blablalablaba", "testvalue"); db.Clear(); // initialize key index

        Assert.False(db.Exists(key));
        Assert.Throws<MO.MODB.Exceptions.KeyNotFoundException>(() => db.First("key", key));

        db.Set(key, value);
        Assert.True(db.Exists(key));
        Assert.Equal(value, db.First("key", key));

        db.Clear();
    }

    [Fact]
    public void CountInIndex_IfNotKeySetThenCountEqualZeroElseNumberOfSets_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "countinindex_test_db"));
        var key = "testKey";
        db.Set("blablalablaba", "testvalue"); db.Clear(); // initialize key index
        
        Assert.Equal(0, db.Count("key", CompareOperators.Equal, key));

        db.Set(key, "testvalue");
        Assert.Equal(1, db.Count("key", CompareOperators.Equal, key));

        db.Clear();
    }

    [Fact]
    public void AnyInIndex_IfNotKeySetThenAnyEqualFalseElseTrue_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "existsinindex_test_db"));
        var key = "testKey";
        db.Set("blablalablaba", "testvalue"); db.Clear(); // initialize key index
        
        Assert.False(db.Any("key", CompareOperators.Equal, key));

        db.Set(key, "testvalue");
        Assert.True(db.Any("key", CompareOperators.Equal, key));

        db.Clear();
    }

    [Fact]
    public void SetStringKeyIndex_WhenCountEqualsZeroIfSetThenSetKeysExistsAndCountEqualsNumberOfSets_IfSetKeyObjectTypeNotEqualIndexTypeAndCanNotBeConvertedThenThrowsKeyIndexTypeMissMatchException_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setString_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(int i = 1; i <= numberOfSets; i++){
            db.Set(i.ToString(), "testvalue", i.ToString().GetType().Name);
            Assert.True(db.Exists(i.ToString()));
        }
        Assert.Equal(numberOfSets, db.Count());

        var key = 11; // Int32 can not be converted to string
        Assert.Throws<MO.MODB.Exceptions.KeyIndexTypeMissMatchException>(() => db.Set(key, "testvalue", typeof(string).Name));
        db.Clear();
    }

    [Fact]
    public void SetInt16KeyIndex_WhenCountEqualsZeroIfSetThenSetKeysExistsAndCountEqualsNumberOfSets_IfSetKeyObjectTypeNotEqualIndexTypeAndCanNotBeConvertedThenThrowsKeyIndexTypeMissMatchException_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setInt16_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(short i = 1; i <= numberOfSets; i++){
            db.Set(i, "testvalue", i.GetType().Name);
            Assert.True(db.Exists(i));
        }
        Assert.Equal(numberOfSets, db.Count());
        
        int key = 10000000; // can not be converted to Int16 (-32768 >= Int16 <= 32767)
        Assert.Throws<MO.MODB.Exceptions.KeyIndexTypeMissMatchException>(() => db.Set(key, "testvalue", typeof(short).Name));

        key = 11; // can be converted to Int16
        db.Set(key, "testvalue", typeof(short).Name);
        Assert.True(db.Exists(key));
        Assert.True(db.Exists(Convert.ToInt16(11)));
        Assert.Equal(db.Get(Convert.ToInt16(11)), db.Get(key));

        db.Clear();
    }

    [Fact]
    public void SetInt32KeyIndex_WhenCountEqualsZeroIfSetThenSetKeysExistsAndCountEqualsNumberOfSets_IfSetKeyObjectTypeNotEqualIndexTypeAndCanNotBeConvertedThenThrowsKeyIndexTypeMissMatchException_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setInt32_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(int i = 1; i <= numberOfSets; i++){
            db.Set(i, "testvalue", i.GetType().Name);
            Assert.True(db.Exists(i));
        }
        Assert.Equal(numberOfSets, db.Count());

        long key = 2147483648; // can not be converted to Int32 (-2,147,483,647 >= Int32 <= 2,147,483,647)
        Assert.Throws<MO.MODB.Exceptions.KeyIndexTypeMissMatchException>(() => db.Set(key, "testvalue", typeof(int).Name));

        key = 11; // can be converted to Int32
        db.Set(key, "testvalue", typeof(int).Name);
        Assert.True(db.Exists(key));
        Assert.True(db.Exists(11));
        Assert.Equal(db.Get(11), db.Get(key));

        db.Clear();
    }

    [Fact]
    public void SetInt64KeyIndex_WhenCountEqualsZeroIfSetThenSetKeysExistsAndCountEqualsNumberOfSets_IfSetKeyObjectTypeNotEqualIndexTypeAndCanNotBeConvertedThenThrowsKeyIndexTypeMissMatchException_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setInt64_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(long i = 1; i <= numberOfSets; i++){
            db.Set(i, "testvalue", i.GetType().Name);
            Assert.True(db.Exists(i));
        }
        Assert.Equal(numberOfSets, db.Count());

        string key = "test"; // can not be converted to Int64
        Assert.Throws<MO.MODB.Exceptions.KeyIndexTypeMissMatchException>(() => db.Set(key, "testvalue", typeof(long).Name));
        key = 11.ToString(); // can be converted to Int64
        db.Set(key, "testvalue", typeof(long).Name);
        Assert.True(db.Exists(key));
        Assert.True(db.Exists(11));
        Assert.Equal(db.Get(11), db.Get(key));

        db.Clear();
    }

    [Fact]
    public void SetDoubleKeyIndex_WhenCountEqualsZeroIfSetThenSetKeysExistsAndCountEqualsNumberOfSets_IfSetKeyObjectTypeNotEqualIndexTypeAndCanNotBeConvertedThenThrowsKeyIndexTypeMissMatchException_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setDouble_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(double i = 1; i <= numberOfSets; i++){
            db.Set(i, "testvalue", i.GetType().Name);
            Assert.True(db.Exists(i));
        }
        Assert.Equal(numberOfSets, db.Count());

        string key = "test"; // can not be converted to Double
        Assert.Throws<MO.MODB.Exceptions.KeyIndexTypeMissMatchException>(() => db.Set(key, "testvalue", typeof(double).Name));
        key = 11.ToString(); // can be converted to Double
        db.Set(key, "testvalue", typeof(double).Name);
        Assert.True(db.Exists(key));
        Assert.True(db.Exists(11));
        Assert.Equal(db.Get(11), db.Get(key));

        db.Clear();
    }

    [Fact]
    public void SetBooleanKeyIndex_ThrowsNotSupportedIndexTypeException_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setBoolean_test_db"));

        Assert.Equal(0, db.Count());

        Assert.Throws<MO.MODB.Exceptions.NotSupportedIndexTypeException>(() => db.Set(false, "testvalue", typeof(bool).Name));

        db.Clear();
    }

    [Fact]
    public void SetWithStringIndex_WhenCountEqualsZeroIfSetThenSetIndexValuesExistsAndCountEqualsNumberOfSets_IfSetWithIndexObjectTypeNotEqualIndexTypeAndCanNotBeConvertedThenThrowsIndexTypeMissMatchException_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setString_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(int i = 1; i <= numberOfSets; i++){
            db.Set(i.ToString(), "testvalue", i.ToString().GetType().Name, new InsertIndexItem(name: "index1", value: i.ToString(), type: i.ToString().GetType().Name));
            Assert.True(db.Any("index1", CompareOperators.Equal, i.ToString()));
        }
        Assert.Equal(numberOfSets, db.Count());

        var key = 11; // Int32 can not be converted to string
        Assert.Throws<MO.MODB.Exceptions.KeyIndexTypeMissMatchException>(() => db.Set(key.ToString(), "testvalue", typeof(string).Name, new InsertIndexItem(name: "index1", value: key, type: typeof(string).Name)));
        
        db.Clear();
    }

    [Fact]
    public void SetWithInt16Index_WhenCountEqualsZeroIfSetThenSetIndexValuesExistsAndCountEqualsNumberOfSets_IfSetWithIndexObjectTypeNotEqualIndexTypeAndCanNotBeConvertedThenThrowsIndexTypeMissMatchException_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setInt16_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(short i = 1; i <= numberOfSets; i++){
            db.Set(i, "testvalue", i.GetType().Name, new InsertIndexItem(name: "index1", value: i, type: i.GetType().Name));
            Assert.True(db.Any("index1", CompareOperators.Equal, i));
        }
        Assert.Equal(numberOfSets, db.Count());
        
        int key = 10000000; // can not be converted to Int16 (-32768 >= Int16 <= 32767)
        Assert.Throws<MO.MODB.Exceptions.KeyIndexTypeMissMatchException>(() => db.Set(11, "testvalue", typeof(short).Name, new InsertIndexItem(name: "index1", value: key, type: typeof(short).Name)));

        key = 11; // can be converted to Int16
        db.Set(11, "testvalue", typeof(short).Name, new InsertIndexItem(name: "index1", value: key, type: typeof(short).Name));
        Assert.True(db.Any("index1", CompareOperators.Equal, key));
        Assert.True(db.Any("index1", CompareOperators.Equal, Convert.ToInt16(key)));
        Assert.Equal(db.First("index1", key), db.First("index1", Convert.ToInt16(key)));

        db.Clear();
    }

    [Fact]
    public void SetWithInt32Index_WhenCountEqualsZeroIfSetThenSetIndexValuesExistsAndCountEqualsNumberOfSets_IfSetWithIndexObjectTypeNotEqualIndexTypeAndCanNotBeConvertedThenThrowsIndexTypeMissMatchException_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setInt32_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(int i = 1; i <= numberOfSets; i++){
            db.Set(i, "testvalue", i.GetType().Name, new InsertIndexItem("index1", i, i.GetType().Name));
            Assert.True(db.Any("index1", CompareOperators.Equal, i));
        }
        Assert.Equal(numberOfSets, db.Count());

        long key = 2147483648; // can not be converted to Int32 (-2,147,483,647 >= Int32 <= 2,147,483,647)
        Assert.Throws<MO.MODB.Exceptions.KeyIndexTypeMissMatchException>(() => db.Set(11, "testvalue", typeof(int).Name, new InsertIndexItem("index1", key, typeof(int).Name)));

        key = 11; // can be converted to Int32
        db.Set(11, "testvalue", typeof(int).Name, new InsertIndexItem("index1", key, typeof(int).Name));
        Assert.True(db.Any("index1", CompareOperators.Equal, key));
        Assert.True(db.Any("index1", CompareOperators.Equal, Convert.ToInt32(key)));
        Assert.Equal(db.First("index1", key), db.First("index1", Convert.ToInt32(key)));

        db.Clear();
    }

    [Fact]
    public void SetWithInt64Index_WhenCountEqualsZeroIfSetThenSetIndexValuesExistsAndCountEqualsNumberOfSets_IfSetWithIndexObjectTypeNotEqualIndexTypeAndCanNotBeConvertedThenThrowsIndexTypeMissMatchException_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setInt64_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(long i = 1; i <= numberOfSets; i++){
            db.Set(i, "testvalue", i.GetType().Name, new InsertIndexItem("index1", i, i.GetType().Name));
            Assert.True(db.Any("index1", CompareOperators.Equal, i));
        }
        Assert.Equal(numberOfSets, db.Count());

        string key = "test"; // can not be converted to Int64
        Assert.Throws<MO.MODB.Exceptions.KeyIndexTypeMissMatchException>(() => db.Set(11, "testvalue", typeof(long).Name, new InsertIndexItem("index1", key, typeof(long).Name)));
        key = 11.ToString(); // can be converted to Int64
        db.Set(11, "testvalue", typeof(long).Name, new InsertIndexItem("index1", key, typeof(long).Name));
        Assert.True(db.Any("index1", CompareOperators.Equal, key));
        Assert.True(db.Any("index1", CompareOperators.Equal, Convert.ToInt64(key)));
        Assert.Equal(db.First("index1", key), db.First("index1", Convert.ToInt64(key)));

        db.Clear();
    }

    [Fact]
    public void SetWithDoubleIndex_WhenCountEqualsZeroIfSetThenSetIndexValuesExistsAndCountEqualsNumberOfSets_IfSetWithIndexObjectTypeNotEqualIndexTypeAndCanNotBeConvertedThenThrowsIndexTypeMissMatchException_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setInt64_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(double i = 1.0; i <= numberOfSets; i++){
            db.Set(i, "testvalue", i.GetType().Name, new InsertIndexItem("index1", i, i.GetType().Name));
            Assert.True(db.Any("index1", CompareOperators.Equal, i));
        }
        Assert.Equal(numberOfSets, db.Count());

        string key = "test"; // can not be converted to Double
        Assert.Throws<MO.MODB.Exceptions.KeyIndexTypeMissMatchException>(() => db.Set(11.0, "testvalue", typeof(double).Name, new InsertIndexItem("index1", key, typeof(double).Name)));
        key = 11.0.ToString(); // can be converted to Double
        db.Set(11.0, "testvalue", typeof(double).Name, new InsertIndexItem("index1", key, typeof(double).Name));
        Assert.True(db.Any("index1", CompareOperators.Equal, key));
        Assert.True(db.Any("index1", CompareOperators.Equal, Convert.ToDouble(key)));
        Assert.Equal(db.First("index1", key), db.First("index1", Convert.ToDouble(key)));

        db.Clear();
    }
}