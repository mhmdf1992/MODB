namespace MODB.Tests;

public class MODBPerformanceTests
{
    IDB _db;
    public MODBPerformanceTests(){
        _db = new DB(Path.Combine(Directory.GetCurrentDirectory(), "key_val_db"));
        SeedDB();
    }

    [Theory]
    [InlineData(99999999)]
    [InlineData(98987789)]
    [InlineData(84481811)]
    public void AnyFilter_100MillionRecordInLessThan1000msTest(int value)
    {
        var res = StopWatch( ()=> _db.Any("key", CompareOperators.Equal, value));
        Assert.True(res.Result);
        Assert.True(res.ProcessingTime < 1000, $"Filter \nResult: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Theory]
    [InlineData(99999999)]
    [InlineData(98987789)]
    [InlineData(84481811)]
    public void CountFilter_100MillionRecordInLessThan1000msTest(int value)
    {
        var res = StopWatch( ()=> _db.Count("key", CompareOperators.Equal, value));
        Assert.Equal(1, res.Result);
        Assert.True(res.ProcessingTime < 1000, $"Filter \nResult: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Theory]
    [InlineData(99999999)]
    [InlineData(98987789)]
    [InlineData(84481811)]
    public void Filter_100MillionRecordInLessThan1000msTest(int value)
    {
        var res = StopWatch( ()=> _db.Filter("key", CompareOperators.Equal, value));
        Assert.NotEmpty(res.Result.Items);
        Assert.True(res.ProcessingTime < 1000, $"Filter \nResult ItemsCount: {res.Result.Items.Count()}\nResult Page: {res.Result.Page}\nResult PageSize: {res.Result.PageSize}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Theory]
    [InlineData(99999999)]
    [InlineData(98987789)]
    [InlineData(84481811)]
    public void Exists_Key100MillionRecordInLessThan1000msTest(int key)
    {
        var res = StopWatch( ()=> _db.Exists(key));
        Assert.True(res.Result);
        Assert.True(res.ProcessingTime < 1000, $"Exists {key}\n Result: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Fact]
    public void Any_100MillionRecordInLessThan5msTest()
    {
        var res = StopWatch( ()=> _db.Any());
        Assert.True(res.Result);
        Assert.True(res.ProcessingTime < 5, $"Any\n Result: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Fact]
    public void Count_100MillionRecordInLessThan5msTest()
    {
        var res = StopWatch( ()=> _db.Count());
        Assert.Equal(100000000, res.Result);
        Assert.True(res.ProcessingTime < 5, $"Count all\n Result: {res.Result}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Fact]
    public void All_100MillionRecordInLessThan5msTest()
    {
        var res = StopWatch( ()=> _db.All());
        Assert.True(res.ProcessingTime < 5, $"Fetch all\nResult TotalItems: {_db.Count()}\nResult ItemsCount: {res.Result.Items.Count()}\nResult Page: {res.Result.Page}\nResult PageSize: {res.Result.PageSize}\nElapsed Time {res.ProcessingTime}ms");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    [InlineData(100000)]
    [InlineData(1000000)]
    [InlineData(10000000)]
    [InlineData(99000000)]
    [InlineData(100000000)]
    public void Get_ValueByKeyFrom100MillionRecordInLessThan1000msTest(int key)
    {
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
        if(!_db.Any()){
            var random = new Random();
            var hash = new Dictionary<object, string>();
            var nameIndexHash = new InsertIndexHash("Name", typeof(string).Name, new Dictionary<object,object>());
            var ageIndexHash = new InsertIndexHash("Age", typeof(short).Name, new Dictionary<object,object>());
            var descriptionIndexHash = new InsertIndexHash("Description", typeof(string).Name, new Dictionary<object,object>());
            for(int i = 1; i <= 100000000; i ++){
                var name = GenerateName(10);
                var age = random.Next(1, 100);
                var description = $"{name} is {age} years old";
                hash.Add(i, "{\"Id\":\"" + i + " \",\"Name\":\"" + name + "\",\"Age\":\"" + age + "\",\"Description\":\"" + description + "\" }");
                nameIndexHash.Hash.Add(i, name);
                ageIndexHash.Hash.Add(i, age);
                descriptionIndexHash.Hash.Add(i, description);
                if(i % 1000000 == 0){
                    _db.InsertHash(hash, typeof(int).Name, nameIndexHash, ageIndexHash, descriptionIndexHash);
                    hash = new Dictionary<object, string>();
                    nameIndexHash = new InsertIndexHash("Name", typeof(string).Name, new Dictionary<object,object>());
                    ageIndexHash = new InsertIndexHash("Age", typeof(short).Name, new Dictionary<object,object>());
                    descriptionIndexHash = new InsertIndexHash("Description", typeof(string).Name, new Dictionary<object,object>());
                }
            }
        }
    }
}