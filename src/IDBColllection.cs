using System.Collections.Generic;

namespace MO.MODB{
    public interface IDBCollection{
        bool Exists(string name);
        IDB Get(string name, bool generateIfNotExists = true);
        IEnumerable<IDB> All();
        void Delete(string name);
    }
}