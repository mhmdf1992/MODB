using System.Text;

namespace MODB.Tests;

public class MODBTests
{
    IDB _db;
    public MODBTests(){
        _db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "key_val_db"));
    }

    [Fact]
    public void SetThenAnyReturnTrue_ClearThenAnyFalseTest()
    {
        _db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "test_db"));
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
        _db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "test_db"));
        _db.Set("test1", "ndfiuhnsiufbniudbnf");
        _db.Set("test2", "ndfiuhnsiufbniudbnf");
        _db.Set("test3", "ndfiuhnsiufbniudbnf");
        _db.Set("test4", "ndfiuhnsiufbniudbnf");
        _db.Set("test5", "ndfiuhnsiufbniudbnf");
        var res = _db.All();
        Assert.All(res.Items, x => x.Equals("ndfiuhnsiufbniudbnf"));
        Assert.True(res.TotalItems == 5);
        _db.Clear();
    }

    [Fact]
    public void GetReturnSetValueTest()
    {
        _db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "test_db"));
        var key = "test";
        var value = "ndfiuhnsiufbniudbnf";
        _db.Set(key, value);
        Assert.Equal(value, _db.Get(key));
        _db.Delete(key);
    }

    [Fact]
    public void GetStreamReturnSetStreamValueTest()
    {
        _db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "test_db"));
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
        _db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "test_db"));
        var key = "test";
        Assert.True(!_db.Any());
        _db.Set(key, "ndfiuhnsiufbniudbnf");
        Assert.True(_db.Any());
        _db.Delete(key);
    }

    [Fact]
    public void KeyExistsFalseIfNotSetElseTrue_KeyExistsFalseIfDeletedTest()
    {
        _db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "test_db"));
        var key = "test";
        Assert.True(!_db.Any());
        _db.Set(key, "ndfiuhnsiufbniudbnf");
        Assert.True(_db.Any());
        Assert.True(_db.Exists(key));
        _db.Delete(key);
        Assert.True(!_db.Exists(key));
        Assert.True(!_db.Any());
    }

    [Theory]
    [InlineData("1")]
    [InlineData("10")]
    [InlineData("100")]
    [InlineData("1000")]
    [InlineData("10000")]
    [InlineData("100000")]
    [InlineData("1000000")]
    [InlineData("10000000")]
    [InlineData("99000000")]
    [InlineData("100000000")]
    public void GetValueByKeyFrom100MillionRecordInLessThan1000msTest(string key)
    {
        Assert.Equal(100000000, _db.Count());
        var res = StopWatch( ()=> _db.Get(key));
        Assert.True(res.ProcessingTime < 1000, $"Key {key} found\nResult: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Fact]
    public void InsertHash100MillionRecordIfAnyFalse_CountEquals100MillionTest()
    {
        if(!_db.Any()){
            var random = new Random();
            var hash = new Dictionary<string, string>();
            for(int i = 1; i <= 20000000; i ++){
                hash.Add(i.ToString(), "{\"Id\":\"" + i + " \",\"FName\":\"" + GenerateName(4) + "\",\"LName\":\"" + GenerateName(6) + "\",\"Age\":\"" + random.Next(20, 100) + "\" }");
            }
            _db.InsertHash(hash);
            hash = new Dictionary<string, string>();
            for(int i = 20000001; i <= 40000000; i ++){
                hash.Add(i.ToString(), "{\"Id\":\"" + i + " \",\"FName\":\"" + GenerateName(4) + "\",\"LName\":\"" + GenerateName(6) + "\",\"Age\":\"" + random.Next(20, 100) + "\" }");
            }
            _db.InsertHash(hash);
            hash = new Dictionary<string, string>();
            for(int i = 40000001; i <= 60000000; i ++){
                hash.Add(i.ToString(), "{\"Id\":\"" + i + " \",\"FName\":\"" + GenerateName(4) + "\",\"LName\":\"" + GenerateName(6) + "\",\"Age\":\"" + random.Next(20, 100) + "\" }");
            }
            _db.InsertHash(hash);
            hash = new Dictionary<string, string>();
            for(int i = 60000001; i <= 80000000; i ++){
                hash.Add(i.ToString(), "{\"Id\":\"" + i + " \",\"FName\":\"" + GenerateName(4) + "\",\"LName\":\"" + GenerateName(6) + "\",\"Age\":\"" + random.Next(20, 100) + "\" }");
            }
            _db.InsertHash(hash);
            hash = new Dictionary<string, string>();
            for(int i = 80000001; i <= 100000000; i ++){
                hash.Add(i.ToString(), "{\"Id\":\"" + i + " \",\"FName\":\"" + GenerateName(4) + "\",\"LName\":\"" + GenerateName(6) + "\",\"Age\":\"" + random.Next(20, 100) + "\" }");
            }
            _db.InsertHash(hash);
        }
        Assert.Equal(100000000, _db.Count());
    }

    static DBResponse<T> StopWatch<T>(Func<T> func){
        var timer = new System.Diagnostics.Stopwatch();
        timer.Start();
        var res = func();
        timer.Stop();
        return new DBResponse<T>(){Result = res, ProcessingTime = timer.ElapsedMilliseconds};
    }

    public struct DBResponse<T>{
        public T Result {get; set;}
        public long ProcessingTime {get; set;}
    }

    public static string GenerateName(int len)
    { 
        Random r = new Random();
        string[] consonants = { "b", "c", "d", "f", "g", "h", "j", "k", "l", "m", "l", "n", "p", "q", "r", "s", "sh", "zh", "t", "v", "w", "x" };
        string[] vowels = { "a", "e", "i", "o", "u", "ae", "y" };
        string Name = "";
        Name += consonants[r.Next(consonants.Length)].ToUpper();
        Name += vowels[r.Next(vowels.Length)];
        int b = 2; //b tells how many times a new letter has been added. It's 2 right now because the first two letters are already in the name.
        while (b < len)
        {
            Name += consonants[r.Next(consonants.Length)];
            b++;
            Name += vowels[r.Next(vowels.Length)];
            b++;
        }
        return Name;
     }
}