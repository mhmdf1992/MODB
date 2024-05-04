namespace MO.MODB.Exceptions{
    public class CompareOperatorIndexTypeMissMatchException : System.Exception{
        CompareOperators _compareOperator;
        string _indexType;
        public CompareOperatorIndexTypeMissMatchException(CompareOperators compareOperator, string indexType): base($"{compareOperator} is not valid to compare {indexType}"){
            _indexType = indexType;
            _compareOperator = compareOperator;
        }
    }
}