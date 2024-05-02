namespace MODB.Tests;

public class MODBPerformanceTests
{
    IDB _db;
    public MODBPerformanceTests(){
        _db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "key_val_db"));
        SeedDB();
    }

    [Theory]
    [InlineData("male")]
    public void AnyContainTest(string pattern)
    {
        var res = StopWatch( ()=> _db.Any("sex", pattern, CompareOperations.Contain));
        Assert.True(res.Result);
        Assert.True(res.ProcessingTime < 5, $"Filter \nResult TotalItems: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Theory]
    [InlineData("male")]
    public void AnyEqualTest(string pattern)
    {
        var res = StopWatch( ()=> _db.Any("sex", pattern, CompareOperations.Equal));
        Assert.True(res.Result);
        Assert.True(res.ProcessingTime < 5, $"Filter \nResult TotalItems: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Fact]
    public void CountFilterEqualTest()
    {
        var res = StopWatch( ()=> _db.Count("sex", "male", CompareOperations.Equal));
        Assert.Equal(100000000, res.Result);
        Assert.True(res.ProcessingTime < 3000, $"Filter \nResult TotalItems: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Fact]
    public void CountFilterContainTest()
    {
        var res = StopWatch( ()=> _db.Count("sex", "male", CompareOperations.Contain));
        Assert.Equal(100000000, res.Result);
        Assert.True(res.ProcessingTime < 3000, $"Filter \nResult TotalItems: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Theory]
    [InlineData("male")]
    public void FilterContainTest(string pattern)
    {
        var res = StopWatch( ()=> _db.Filter("sex", pattern, CompareOperations.Contain));
        foreach(var item in res.Result.Items){
            Assert.Contains(pattern, item);
        }
        var count = _db.Count("sex", pattern, CompareOperations.Contain);
        Assert.Equal(100000000, count);
        Assert.True(res.ProcessingTime < 5, $"Filter \nResult TotalItems: {count}\nResult ItemsCount: {res.Result.Items.Count()}\nResult Page: {res.Result.Page}\nResult PageSize: {res.Result.PageSize}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Theory]
    [InlineData("male")]
    public void FilterEqualTest(string pattern)
    {
        var res = StopWatch( ()=> _db.Filter("sex", pattern, CompareOperations.Equal));
        foreach(var item in res.Result.Items){
            Assert.Contains(pattern, item);
        }
        var count = _db.Count("sex", pattern, CompareOperations.Contain);
        Assert.Equal(100000000, count);
        Assert.True(res.ProcessingTime < 5, $"Filter \nResult TotalItems: {count}\nResult ItemsCount: {res.Result.Items.Count()}\nResult Page: {res.Result.Page}\nResult PageSize: {res.Result.PageSize}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Theory]
    [InlineData("99999999")]
    [InlineData("98987789")]
    [InlineData("84481811")]
    public void ExistsTest(string key)
    {
        var res = StopWatch( ()=> _db.Exists(key));
        Assert.True(res.Result);
        Assert.True(res.ProcessingTime < 1000, $"Exists {key}\n Result: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
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
        var count = _db.Count();
        Assert.Equal(100000000, count);
        Assert.True(res.ProcessingTime < 5, $"Fetch all\nResult TotalItems: {count}\nResult ItemsCount: {res.Result.Items.Count()}\nResult Page: {res.Result.Page}\nResult PageSize: {res.Result.PageSize}\nElapsed Time {res.ProcessingTime}ms");
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

    static readonly string[] SEX_ARRAY = new string[] {"male", "female"};
    static string GenerateName(int len)
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

    void SeedDB(){
        if(!_db.Any() || _db.Count() < 100000000){
            var random = new Random();
            var hash = new Dictionary<string, string>();
            var fnameIndexHash = new InsertIndexHash("FName", new Dictionary<string,string>());
            var lnameIndexHash = new InsertIndexHash("LNname", new Dictionary<string,string>());
            var ageIndexHash = new InsertIndexHash("Age", new Dictionary<string,string>());
            var sexIndexHash = new InsertIndexHash("Sex", new Dictionary<string,string>());
            var descriptionIndexHash = new InsertIndexHash("Description", new Dictionary<string,string>());
            for(int i = _db.Count() + 1; i <= 100000000; i ++){
                var fname = GenerateName(4);
                var lname = GenerateName(6);
                var age = random.Next(20, 100);
                var sex = SEX_ARRAY[random.Next(0, 1)];
                var description = $"{fname} {lname} is a {sex} {age} years old";
                hash.Add(i.ToString(), "{\"Id\":\"" + i + " \",\"FName\":\"" + fname + "\",\"LName\":\"" + lname + "\",\"Age\":\"" + age + "\",\"Sex\":\"" + sex + "\",\"Description\":\"" + description + "\" }");
                fnameIndexHash.Hash.Add(i.ToString(), fname);
                lnameIndexHash.Hash.Add(i.ToString(), lname);
                ageIndexHash.Hash.Add(i.ToString(), age.ToString());
                sexIndexHash.Hash.Add(i.ToString(), sex);
                descriptionIndexHash.Hash.Add(i.ToString(), description);
                if(i % 10000000 == 0){
                    _db.InsertHash(hash, fnameIndexHash, lnameIndexHash, ageIndexHash, sexIndexHash, descriptionIndexHash);
                    hash = new Dictionary<string, string>();
                    fnameIndexHash = new InsertIndexHash("FName", new Dictionary<string,string>());
                    lnameIndexHash = new InsertIndexHash("LNname", new Dictionary<string,string>());
                    ageIndexHash = new InsertIndexHash("Age", new Dictionary<string,string>());
                    sexIndexHash = new InsertIndexHash("Sex", new Dictionary<string,string>());
                    descriptionIndexHash = new InsertIndexHash("Description", new Dictionary<string,string>());
                }
            }
        }
    }
}