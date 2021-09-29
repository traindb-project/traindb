/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar TrainDBSql;

traindbStmts
    : createModel
    | dropModel
    | trainModelInstance
    | dropModelInstance
    | showStmt
    ;

createModel
    : K_CREATE K_MODEL modelName K_TYPE modelType modelLocation modelUri
    ;

dropModel
    : K_DROP K_MODEL modelName
    ;

trainModelInstance
    : K_TRAIN K_MODEL modelName K_INSTANCE modelInstanceName K_ON qualifiedTableName '(' columnNameList ')'
    ;

dropModelInstance
    : K_DROP K_MODEL K_INSTANCE modelInstanceName
    ;

modelName
    : IDENTIFIER
    ;

modelType
    : K_INFERENCE
    | K_SYNOPSIS
    ;

modelLocation
    : K_LOCAL
    | K_REMOTE
    ;

modelUri
    : STRING_LITERAL
    ;

showStmt
    : K_SHOW showTargets
    ;

showTargets
    : K_MODELS  # ShowModels
    | K_MODEL modelName K_INSTANCES  # ShowModelInstances
    ;

modelInstanceName
    : IDENTIFIER
    ;

qualifiedTableName
    : schemaName '.' tableName
    ;

schemaName
    : IDENTIFIER
    ;

tableName
    : IDENTIFIER
    ;

columnNameList
    : columnName ( ',' columnName )*
    ;

columnName
    : IDENTIFIER
    ;

error
    : UNEXPECTED_CHAR
        {
            throw new RuntimeException("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text);
        }
    ;

K_AS : A S ;
K_CREATE : C R E A T E ;
K_DROP : D R O P ;
K_INFERENCE : I N F E R E N C E ;
K_INSTANCE : I N S T A N C E ;
K_INSTANCES : I N S T A N C E S ;
K_LOCAL : L O C A L ;
K_MODEL : M O D E L ;
K_MODELS : M O D E L S ;
K_ON : O N ;
K_REMOTE : R E M O T E ;
K_SHOW : S H O W ;
K_SYNOPSIS : S Y N O P S I S ;
K_TRAIN : T R A I N ;
K_TYPE : T Y P E ;

IDENTIFIER
    : '"' ( ~["\r\n] | '""' )* '"'
        {
            setText(getText().substring(1, getText().length() - 1).replace("\"\"", "\""));
        }
    | '`' ( ~[`\r\n] | '``' )* '`'
        {
            setText(getText().substring(1, getText().length() - 1).replace("``", "`"));
        }
    | '[' ( ~[\]\r\n]* | ']]' )* ']'
        {
            setText(getText().substring(1, getText().length() - 1).replace("]]", "]"));
        }
    | LETTER ( LETTER | DIGIT )*
        {
            setText(getText().toLowerCase());
        }
    ;

NUMERIC_LITERAL
    : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
    | '.' DIGIT+ ( E [-+]? DIGIT+ )?
    ;

STRING_LITERAL
    : '\'' ( ~['\r\n] | '\'\'' )* '\''
        {
            setText(getText().substring(1, getText().length() - 1).replace("''", "'"));
        }
    ;

WHITESPACES : [ \t\r\n]+ -> channel(HIDDEN) ;

UNEXPECTED_CHAR : . ;

fragment A : [aA] ;
fragment B : [bB] ;
fragment C : [cC] ;
fragment D : [dD] ;
fragment E : [eE] ;
fragment F : [fF] ;
fragment G : [gG] ;
fragment H : [hH] ;
fragment I : [iI] ;
fragment J : [jJ] ;
fragment K : [kK] ;
fragment L : [lL] ;
fragment M : [mM] ;
fragment N : [nN] ;
fragment O : [oO] ;
fragment P : [pP] ;
fragment Q : [qQ] ;
fragment R : [rR] ;
fragment S : [sS] ;
fragment T : [tT] ;
fragment U : [uU] ;
fragment V : [vV] ;
fragment W : [wW] ;
fragment X : [xX] ;
fragment Y : [yY] ;
fragment Z : [zZ] ;

fragment LETTER : [a-zA-Z_] ;
fragment DIGIT : [0-9] ;
