namespace MO.MODB.Exceptions{
    public class CompareOperatorIndexTypeMissMatchException : System.ArgumentException{
        CompareOperators _compareOperator;
        string _indexType;
        public CompareOperatorIndexTypeMissMatchException(CompareOperators compareOperator, string indexType): base(message: $"{compareOperator} is not valid to compare {indexType}", paramName: "compareOperator"){
            _indexType = indexType;
            _compareOperator = compareOperator;
        }
    }
}