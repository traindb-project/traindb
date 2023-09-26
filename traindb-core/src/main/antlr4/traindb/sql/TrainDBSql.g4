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

@lexer::header {
    import org.apache.calcite.avatica.util.Casing;
    import org.apache.calcite.sql.parser.SqlParser;
}

@lexer::members {
    SqlParser.Config parserConfig;

    public TrainDBSqlLexer(CharStream input, SqlParser.Config parserConfig) {
        this(input);
        this.parserConfig = parserConfig;
    }
}

traindbStmts
    : createModeltype
    | dropModeltype
    | trainModel
    | dropModel
    | alterModel
    | createSynopsis
    | dropSynopsis
    | alterSynopsis
    | showStmt
    | useSchema
    | describeTable
    | bypassDdlStmt
    | deleteQueryLogs
    | deleteTasks
    | exportModel
    | importModel
    ;

createModeltype
    : K_CREATE K_MODELTYPE modeltypeName K_FOR modeltypeCategory K_AS modeltypeSpecClause
    ;

dropModeltype
    : K_DROP K_MODELTYPE modeltypeName
    ;

trainModel
    : K_TRAIN K_MODEL modelName K_MODELTYPE modeltypeName K_ON tableName '(' columnNameList ')' trainModelOptionsClause?
    ;

dropModel
    : K_DROP K_MODEL modelName
    ;

alterModel
    : K_ALTER K_MODEL modelName alterModelClause
    ;

alterModelClause
    : K_RENAME K_TO newModelName
    | enableDisableClause
    ;

enableDisableClause
    : K_ENABLE
    | K_DISABLE
    ;

deleteQueryLogs
    : K_DELETE K_QUERYLOGS limitNumber
    ;

deleteTasks
    : K_DELETE K_TASKS limitNumber
    ;

modeltypeName
    : IDENTIFIER
    ;

modeltypeCategory
    : K_INFERENCE
    | K_SYNOPSIS
    ;

modeltypeSpecClause
    : modeltypeLocation K_CLASS modeltypeClassName K_IN modeltypeUri
    ;

modeltypeLocation
    : K_LOCAL
    | K_REMOTE
    ;

modeltypeClassName
    : STRING_LITERAL
    ;

modeltypeUri
    : STRING_LITERAL
    ;

trainModelOptionsClause
    : K_OPTIONS '(' optionKeyValueList ')'
    ;

optionKeyValueList
    : optionKeyValue ( ',' optionKeyValue )*
    ;

optionKeyValue
    : optionKey '=' optionValue
    ;

optionKey
    : STRING_LITERAL
    ;

optionValue
    : STRING_LITERAL
    | NUMERIC_LITERAL
    ;

exportModel
    : K_EXPORT K_MODEL modelName
    ;

importModel
    : K_IMPORT K_MODEL modelName K_FROM modelBinaryString
    ;

modelBinaryString
    : BINARY_STRING_LITERAL
    ;

showStmt
    : K_SHOW showTargets showWhereClause?
    ;

showTargets
    : K_MODELTYPES
    | K_MODELS
    | K_SYNOPSES
    | K_SCHEMAS
    | K_TABLES
    | K_HYPERPARAMETERS
    | K_TRAININGS
    | K_QUERYLOGS
    | K_TASKS
    ;

showWhereClause
    : K_WHERE showWhereExpressionList
    ;

showWhereExpressionList
    : showWhereExpression ( K_AND showWhereExpression )*
    ;

showWhereExpression
    : showFilterKey showFilterOperator optionValue
    ;

showFilterKey
    : IDENTIFIER
    ;

showFilterOperator
    : K_LIKE
    | '='
    ;

modelName
    : IDENTIFIER
    ;

newModelName
    : IDENTIFIER
    ;

createSynopsis
    : K_CREATE K_SYNOPSIS synopsisName K_FROM K_MODEL modelName K_LIMIT limitNumber
    ;

dropSynopsis
    : K_DROP K_SYNOPSIS synopsisName
    ;

alterSynopsis
    : K_ALTER K_SYNOPSIS synopsisName alterSynopsisClause
    ;

alterSynopsisClause
    : K_RENAME K_TO newSynopsisName
    ;

useSchema
    : K_USE schemaName
    ;

describeTable
    : ( K_DESCRIBE | K_DESC ) tableName
    ;

bypassDdlStmt
    : K_BYPASS ddlString
    ;

schemaName
    : IDENTIFIER
    ;

tableName
    : ( schemaName '.' )? tableIdentifier=IDENTIFIER
    ;

columnNameList
    : columnName ( ',' columnName )*
    ;

columnName
    : IDENTIFIER
    ;

synopsisName
    : IDENTIFIER
    ;

newSynopsisName
    : IDENTIFIER
    ;

limitNumber
    : NUMERIC_LITERAL
    ;

ddlString
    : ( . | WHITESPACES )+
    ;

error
    : UNEXPECTED_CHAR
        {
            throw new RuntimeException("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text);
        }
    ;

K_ALTER : A L T E R ;
K_AND : A N D ;
K_AS : A S ;
K_BYPASS : B Y P A S S ;
K_CLASS : C L A S S ;
K_CREATE : C R E A T E ;
K_DELETE : D E L E T E ;
K_DESC : D E S C ;
K_DESCRIBE : D E S C R I B E ;
K_DISABLE : D I S A B L E ;
K_DROP : D R O P ;
K_ENABLE : E N A B L E ;
K_EXPORT : E X P O R T ;
K_FOR : F O R ;
K_FROM : F R O M ;
K_HYPERPARAMETERS : H Y P E R P A R A M E T E R S ;
K_IMPORT : I M P O R T ;
K_IN : I N ;
K_INFERENCE : I N F E R E N C E ;
K_LIKE : L I K E ;
K_LIMIT : L I M I T ;
K_LOCAL : L O C A L ;
K_MODEL : M O D E L ;
K_MODELS : M O D E L S ;
K_MODELTYPE : M O D E L T Y P E ;
K_MODELTYPES : M O D E L T Y P E S ;
K_ON : O N ;
K_OPTIONS : O P T I O N S ;
K_QUERYLOGS : Q U E R Y L O G S ;
K_REMOTE : R E M O T E ;
K_RENAME : R E N A M E ;
K_SCHEMAS : S C H E M A S ;
K_SHOW : S H O W ;
K_SYNOPSES : S Y N O P S E S ;
K_SYNOPSIS : S Y N O P S I S ;
K_TABLES : T A B L E S ;
K_TASKS : T A S K S ;
K_TO : T O ;
K_TRAIN : T R A I N ;
K_TRAININGS : T R A I N I N G S;
K_USE : U S E ;
K_WHERE : W H E R E ;

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
            setText(TrainDBSql.toCase(getText(), parserConfig.unquotedCasing()));
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

BINARY_STRING_LITERAL
    : 'x' '\'' HEXDIGIT* '\''
        {
            setText(getText().substring(2, getText().length() - 1));
        }
    ;

HEXDIGIT
    : [a-fA-F0-9]
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
