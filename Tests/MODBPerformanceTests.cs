namespace MODB.Tests;

public class MODBPerformanceTests
{
    IDB _db;
    public MODBPerformanceTests(){
        _db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "key_val_db"));
    }

    [Fact]
    public void AnyTest()
    {
        var res = StopWatch( ()=> _db.Any());
        Assert.True(res.Result);
        Assert.True(res.ProcessingTime < 5, $"Any\n Result: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Fact]
    public void CountTest()
    {
        var res = StopWatch( ()=> _db.Count());
        Assert.Equal(100000000, res.Result);
        Assert.True(res.ProcessingTime < 5, $"Count all\n Result: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Fact]
    public void AllTest()
    {
        var res = StopWatch( ()=> _db.All());
        Assert.Equal(100000000, res.Result.TotalItems);
        Assert.True(res.ProcessingTime < 5, $"Fetch all\nResult TotalItems: {res.Result.TotalItems}\nResult ItemsCount: {res.Result.Items.Count()}\nResult Page: {res.Result.Page}\nResult PageSize: {res.Result.PageSize}\nElapsed Time {res.ProcessingTime}ms");
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