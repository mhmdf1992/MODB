using System;

namespace MO.MODB{
    public class KeyIndexBook : KeyIndexBookBase, IIndexBook
    {
        public KeyIndexBook(string name, string indexType, string path) : base(name, indexType, path){}
    }
}