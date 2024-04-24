using System;
using System.Collections.Generic;
using System.Linq;
using MO.MOFile;

namespace MO.MODB{
    public class KeyValueIndexBook : KeyIndexBookBase, IIndexBook, IKeyValueIndexBook
    {
        public KeyValueIndexBook(string name, string path) : base(name, path){
        }
        
        public bool Any(){
            if(!_indexWRs.Any())
                return false;
            return _indexWRs.Values.Any(x => ((IKeyValueIndexWR)x).Any());
        }

        public int Count(){
            if(!_indexWRs.Any())
                return 0;
            return _indexWRs.Values.Sum(x => ((IKeyValueIndexWR)x).Count());
        }

        public bool Exists(string key){
            if(!_indexWRs.Any())
                return false;
            var pattern = _encoding.GetBytes(key);
            if(!_indexWRs.ContainsKey(pattern.Length))
                return false;
            return ((IKeyValueIndexWR)_indexWRs[pattern.Length]).Exists(pattern);
        }

        public IndexItemToRead Find(string key)
        {
            if(!_indexWRs.Any())
                throw new KeyNotFoundException();
            var pattern = _encoding.GetBytes(key);
            if(!_indexWRs.ContainsKey(pattern.Length))
                throw new KeyNotFoundException();
            return ((IKeyValueIndexWR)_indexWRs[pattern.Length]).Find(pattern);
        }

        public IndexItemToDelete Delete(string key)
        {
            if(!_indexWRs.Any())
                throw new KeyNotFoundException(key);
            var pattern = _encoding.GetBytes(key);
            if(!_indexWRs.ContainsKey(pattern.Length))
                throw new KeyNotFoundException(key);
            return ((IKeyValueIndexWR)_indexWRs[pattern.Length]).Delete(pattern);
        }

        public IndexItemToDelete DeleteIfExists(string key)
        {
            if(!_indexWRs.Any())
                return default;
            var pattern = _encoding.GetBytes(key);
            if(!_indexWRs.ContainsKey(pattern.Length))
                return default;
            return ((IKeyValueIndexWR)_indexWRs[pattern.Length]).Delete(pattern);
        }

        public IEnumerable<ReadObject> All()
        {
            if(!_indexWRs.Any())
                return Enumerable.Empty<ReadObject>();
            return _indexWRs.Values.Select(x => ((IKeyValueIndexWR)x).All()).SelectMany(x => x);
        }
    }
}