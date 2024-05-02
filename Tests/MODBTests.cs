using System.Text;

namespace MODB.Tests;

public class MODBTests
{
    IDB _db;
    public MODBTests(){
        _db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "test_db"));
    }

    [Fact]
    public void SetThenDeleteThenSet_CaseSizeOfKeyAndValueEqualsDeletedThenDBSizeEqualTest()
    {
        var db1 = new DB(Path.Combine(Directory.GetCurrentDirectory(), "test_del_db"));
        for(int i = 1; i <= 100; i ++){
            db1.Set($"test{i}", "ndfiuhnsiufbniudbnf");
        }
        var baseSize = db1.Size;
        for(int i = 1; i <= 100; i ++){
            db1.Delete($"test{i}");
        }
        Assert.Equal(baseSize, db1.Size - (20*100));
        for(int i = 1; i <= 100; i ++){
            db1.Set($"test{i}", "ndfiuhnsiufbniudbnf");    
        }
        Assert.Equal(baseSize, db1.Size);
        db1.Clear();
    }

    [Fact]
    public void SetThenDeleteThenSet_CaseInputSizeOfKeyAndValueLessThanDeletedThenDBSizeEqualTest()
    {
        var db1 = new DB(Path.Combine(Directory.GetCurrentDirectory(), "test_del_db"));
        for(int i = 1; i <= 100; i ++){
            db1.Set($"test{i}", "ndfiuhnsiufbniudbnf");
        }
        var baseSize = db1.Size;
        for(int i = 1; i <= 100; i ++){
            db1.Delete($"test{i}");
        }
        Assert.Equal(baseSize, db1.Size - (20*100));
        for(int i = 1; i <= 100; i ++){
            db1.Set($"test{i}", "ndfiuhnsiuf");    
        }
        Assert.Equal(baseSize, db1.Size);
        db1.Clear();
    }

    [Fact]
    public void SetThenAnyReturnTrue_ClearThenAnyFalseTest()
    {
        _db.Set("test1", "ndfiuhnsiufbniudbnf");
        _db.Set("test2", "ndfiuhnsiufbniudbnf");
        _db.Set("test3", "ndfiuhnsiufbniudbnf");
        _db.Set("test4", "ndfiuhnsiufbniudbnf");
        _db.Set("test5", "ndfiuhnsiufbniudbnf");
        Assert.True(_db.Any());
        _db.Clear();
        Assert.False(_db.Any());
    }

    [Fact]
    public void AllReturnAllSetsTest()
    {
        _db.Set("test1", "ndfiuhnsiufbniudbnf");
        _db.Set("test2", "ndfiuhnsiufbniudbnf");
        _db.Set("test3", "ndfiuhnsiufbniudbnf");
        _db.Set("test4", "ndfiuhnsiufbniudbnf");
        _db.Set("test5", "ndfiuhnsiufbniudbnf");
        var res = _db.All();
        Assert.All(res.Items, x => x.Equals("ndfiuhnsiufbniudbnf"));
        Assert.Equal(5, _db.Count());
        _db.Clear();
    }

    [Fact]
    public void GetReturnSetValueTest()
    {
        var key = "test";
        var value = "ndfiuhnsiufbniudbnf";
        _db.Set(key, value);
        Assert.Equal(value, _db.Get(key));
        _db.Delete(key);
    }

    [Fact]
    public void GetStreamReturnSetStreamValueTest()
    {
        var key = "test";
        var value = "ndfiuhnsiufbniudbnf";
        var bytes = Encoding.UTF8.GetBytes(value);
        using var stream = new MemoryStream(bytes);
        _db.SetStream(key, stream);
        using var resStream = _db.GetStream(key);
        using var reader = new StreamReader(resStream);
        Assert.Equal(value, reader.ReadToEnd());
        _db.Delete(key);
    }

    [Fact]
    public void AnyReturnFalseIfDBEmptyElseFalseTest()
    {
        var key = "test";
        Assert.True(!_db.Any());
        _db.Set(key, "ndfiuhnsiufbniudbnf");
        Assert.True(_db.Any());
        _db.Delete(key);
    }

    [Fact]
    public void KeyExistsFalseIfNotSetElseTrue_KeyExistsFalseIfDeletedTest()
    {
        var key = "test";
        Assert.True(!_db.Any());
        _db.Set(key, "ndfiuhnsiufbniudbnf");
        Assert.True(_db.Any());
        Assert.True(_db.Exists(key));
        _db.Delete(key);
        Assert.True(!_db.Exists(key));
        Assert.True(!_db.Any());
    }
}