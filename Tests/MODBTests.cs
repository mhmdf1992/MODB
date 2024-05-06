using System.Text;

namespace MODB.Tests;

public class MODBTests
{

    [Fact]
    public void All_WhenCountEqualZeroAllItemsEmpty_WhenCountGreaterThanZeroAllItemsCountEqualNumberOfSetsEqualCountAndAllItemsContainsSetValues_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "all_test_db"));

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
    public void Delete_WhenExistsEqualFalseThenDeleteThrowsKeyNotFoundException_WhenExistsEqualTrueIfDeleteThenExistsEqualFalse_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "delete_test_db"));
        var key = "testkey";
        var value = "testvalue";

        Assert.False(db.Exists(key));
        Assert.Throws<MO.MODB.Exceptions.KeyNotFoundException>(() => db.Delete(key));

        db.Set(key, value);
        Assert.True(db.Exists(key));
        db.Delete(key);
        Assert.False(db.Exists(key));

        db.Clear();
    }

    [Fact]
    public void GetStream_WhenExistsEqualFalseThenGetThrowsKeyNotFoundException_WhenExistsEqualTrueGetReturnSetValue_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "getstream_test_db"));
        var key = "testkey";
        var value = "testvalue";
        
        Assert.False(db.Exists(key));
        Assert.Throws<MO.MODB.Exceptions.KeyNotFoundException>(() => db.Get(key));

        db.Set(key, value);
        Assert.True(db.Exists(key));
        using var stream = db.GetStream(key);
        using var reader = new StreamReader(stream);
        Assert.Equal(value, reader.ReadToEnd());

        db.Clear();
    }

    [Fact]
    public void Get_WhenExistsEqualFalseThenGetThrowsKeyNotFoundExceptionElseReturnSetValue_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "get_test_db"));
        var key = "testkey";
        var value = "testvalue";

        Assert.False(db.Exists(key));
        Assert.Throws<MO.MODB.Exceptions.KeyNotFoundException>(() => db.Get(key));

        db.Set(key, value);
        Assert.True(db.Exists(key));
        Assert.Equal(value, db.Get(key));

        db.Clear();
    }

    [Fact]
    public void SetStream_WhenCountEqualsZeroIfSetThenSetKeysExistsAndCountEqualsNumberOfSets_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "setstream_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(int i = 1; i <= numberOfSets; i++){
            db.SetStream(i.ToString(), new MemoryStream(Encoding.UTF8.GetBytes("testvalue")));
            Assert.True(db.Exists(i.ToString()));
        }
        Assert.Equal(numberOfSets, db.Count());

        db.Clear();
    }

    [Fact]
    public void Set_WhenCountEqualsZeroIfSetThenSetKeysExistsAndCountEqualsNumberOfSets_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "set_test_db"));

        Assert.Equal(0, db.Count());
        var numberOfSets = 10;

        for(int i = 1; i <= numberOfSets; i++){
            db.Set(i.ToString(), "testvalue");
            Assert.True(db.Exists(i.ToString()));
        }
        Assert.Equal(numberOfSets, db.Count());

        db.Clear();
    }
    
    [Fact]
    public void Exists_IfNotKeySetThenExistsEqualFalseElseTrue_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "exists_test_db"));
        var key = "testKey";

        Assert.False(db.Exists(key));
        
        db.Set(key, "testvalue");
        Assert.True(db.Exists(key));

        db.Clear();
    }

    [Fact]
    public void Clear_WhenCountGreaterThanZeroIfClearThenCountEqualZero_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "clear_test_db"));

        db.Set("testkey", "testvalue");
        Assert.True(db.Count() > 0);

        db.Clear();

        Assert.Equal(0, db.Count());

        db.Clear();
    }

    [Fact]
    public void Count_WhenDBEmptyThenCountEqualZeroElseCountEqualToNumberOfSets_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "count_test_db"));
        Assert.Equal(0, db.Count());

        var numberOfSets = 10;
        for(int i = 1; i <= numberOfSets; i++){
            db.Set(i.ToString(), "testvalue");
        }

        Assert.Equal(numberOfSets, db.Count());

        db.Clear();
    }

    [Fact]
    public void Any_WhenCountEqualZeroThenAnyReturnFalseElseTrue_Test()
    {
        var db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "any_test_db"));
        Assert.Equal(0, db.Count());
        Assert.False(db.Any());

        db.Set("testkey", "testvalue");

        Assert.NotEqual(0, db.Count());
        Assert.True(db.Any());

        db.Clear();
    }
}