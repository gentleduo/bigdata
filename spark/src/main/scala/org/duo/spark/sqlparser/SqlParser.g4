grammar SqlParser;

array : '{' value (',' value)* '}';
value : XX | array;
XX : [0-9]+;
WS : [\t\r\n] -> skip;