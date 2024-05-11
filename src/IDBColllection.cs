namespace MO.MODB{
    public interface IDBCollection{
        bool Exists(string name);
        IDB Get(string name, bool generateIfNotExists = true);
    }
}