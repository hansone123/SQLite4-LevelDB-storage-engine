/* Automatically generated.  Do not edit */
/* See the mkopcodeh.awk script for details */
#define OP_Goto                                 1
#define OP_Gosub                                2
#define OP_Return                               3
#define OP_Yield                                4
#define OP_HaltIfNull                           5
#define OP_Halt                                 6
#define OP_Integer                              7
#define OP_Num                                  8
#define OP_String8                             93   /* same as TK_STRING   */
#define OP_String                               9
#define OP_Null                                10
#define OP_Blob                                11
#define OP_Variable                            12
#define OP_Move                                13
#define OP_Copy                                14
#define OP_SCopy                               15
#define OP_ResultRow                           16
#define OP_Concat                              90   /* same as TK_CONCAT   */
#define OP_Add                                 85   /* same as TK_PLUS     */
#define OP_Subtract                            86   /* same as TK_MINUS    */
#define OP_Multiply                            87   /* same as TK_STAR     */
#define OP_Divide                              88   /* same as TK_SLASH    */
#define OP_Remainder                           89   /* same as TK_REM      */
#define OP_CollSeq                             17
#define OP_KVMethod                            18
#define OP_Mifunction                          20
#define OP_Function                            21
#define OP_BitAnd                              81   /* same as TK_BITAND   */
#define OP_BitOr                               82   /* same as TK_BITOR    */
#define OP_ShiftLeft                           83   /* same as TK_LSHIFT   */
#define OP_ShiftRight                          84   /* same as TK_RSHIFT   */
#define OP_AddImm                              22
#define OP_MustBeInt                           23
#define OP_RealAffinity                        24
#define OP_ToText                             141   /* same as TK_TO_TEXT  */
#define OP_ToBlob                             142   /* same as TK_TO_BLOB  */
#define OP_ToNumeric                          143   /* same as TK_TO_NUMERIC*/
#define OP_ToInt                              144   /* same as TK_TO_INT   */
#define OP_ToReal                             145   /* same as TK_TO_REAL  */
#define OP_Eq                                  75   /* same as TK_EQ       */
#define OP_Ne                                  74   /* same as TK_NE       */
#define OP_Lt                                  78   /* same as TK_LT       */
#define OP_Le                                  77   /* same as TK_LE       */
#define OP_Gt                                  76   /* same as TK_GT       */
#define OP_Ge                                  79   /* same as TK_GE       */
#define OP_Permutation                         25
#define OP_Compare                             26
#define OP_Jump                                27
#define OP_And                                 68   /* same as TK_AND      */
#define OP_Or                                  67   /* same as TK_OR       */
#define OP_Not                                 19   /* same as TK_NOT      */
#define OP_BitNot                              92   /* same as TK_BITNOT   */
#define OP_Once                                28
#define OP_If                                  29
#define OP_IfNot                               30
#define OP_IsNull                              72   /* same as TK_ISNULL   */
#define OP_NotNull                             73   /* same as TK_NOTNULL  */
#define OP_Column                              31
#define OP_MakeKey                             32
#define OP_MakeRecord                          33
#define OP_Affinity                            34
#define OP_Count                               35
#define OP_Savepoint                           36
#define OP_Transaction                         37
#define OP_ReadCookie                          38
#define OP_SetCookie                           39
#define OP_VerifyCookie                        40
#define OP_OpenRead                            41
#define OP_OpenWrite                           42
#define OP_OpenAutoindex                       43
#define OP_OpenEphemeral                       44
#define OP_SorterOpen                          45
#define OP_Close                               46
#define OP_SeekPk                              47
#define OP_SeekLt                              48
#define OP_SeekLe                              49
#define OP_SeekGe                              50
#define OP_SeekGt                              51
#define OP_NotExists                           52
#define OP_NotFound                            53
#define OP_Found                               54
#define OP_IsUnique                            55
#define OP_Sequence                            56
#define OP_NewRowid                            57
#define OP_NewIdxid                            58
#define OP_Delete                              59
#define OP_ResetCount                          60
#define OP_GrpCompare                          61
#define OP_SorterData                          62
#define OP_RowKey                              63
#define OP_RowData                             64
#define OP_AnalyzeKey                          65
#define OP_Rowid                               66
#define OP_NullRow                             69
#define OP_Last                                70
#define OP_SorterSort                          71
#define OP_Sort                                80
#define OP_Rewind                              91
#define OP_SorterNext                          94
#define OP_Prev                                95
#define OP_Next                                96
#define OP_Insert                              97
#define OP_IdxDelete                           98
#define OP_IdxRowkey                           99
#define OP_IdxLT                              100
#define OP_IdxLE                              101
#define OP_IdxGE                              102
#define OP_IdxGT                              103
#define OP_Clear                              104
#define OP_ParseSchema                        105
#define OP_LoadAnalysis                       106
#define OP_DropTable                          107
#define OP_DropIndex                          108
#define OP_DropTrigger                        109
#define OP_RowSetTest                         110
#define OP_RowSetAdd                          111
#define OP_RowSetRead                         112
#define OP_Program                            113
#define OP_Param                              114
#define OP_FkCounter                          115
#define OP_FkIfZero                           116
#define OP_MemMax                             117
#define OP_IfPos                              118
#define OP_IfNeg                              119
#define OP_IfZero                             120
#define OP_AggStep                            121
#define OP_AggFinal                           122
#define OP_JournalMode                        123
#define OP_Expire                             124
#define OP_VBegin                             125
#define OP_VCreate                            126
#define OP_VDestroy                           127
#define OP_VOpen                              128
#define OP_VFilter                            129
#define OP_VColumn                            130
#define OP_VNext                              131
#define OP_VRename                            132
#define OP_VUpdate                            133
#define OP_Trace                              134
#define OP_FtsUpdate                          135
#define OP_FtsCksum                           136
#define OP_FtsOpen                            137
#define OP_FtsNext                            138
#define OP_FtsPk                              139
#define OP_Noop                               140
#define OP_Explain                            146


/* Properties such as "out2" or "jump" that are specified in
** comments following the "case" for each opcode in the vdbe.c
** are encoded into bitvectors as follows:
*/
#define OPFLG_JUMP            0x0001  /* jump:  P2 holds jmp target */
#define OPFLG_OUT2_PRERELEASE 0x0002  /* out2-prerelease: */
#define OPFLG_IN1             0x0004  /* in1:   P1 is an input */
#define OPFLG_IN2             0x0008  /* in2:   P2 is an input */
#define OPFLG_IN3             0x0010  /* in3:   P3 is an input */
#define OPFLG_OUT2            0x0020  /* out2:  P2 is an output */
#define OPFLG_OUT3            0x0040  /* out3:  P3 is an output */
#define OPFLG_INITIALIZER {\
/*   0 */ 0x00, 0x01, 0x01, 0x04, 0x04, 0x10, 0x00, 0x02,\
/*   8 */ 0x02, 0x02, 0x02, 0x02, 0x02, 0x00, 0x24, 0x24,\
/*  16 */ 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x04, 0x05,\
/*  24 */ 0x04, 0x00, 0x00, 0x01, 0x01, 0x05, 0x05, 0x00,\
/*  32 */ 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x02, 0x10,\
/*  40 */ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,\
/*  48 */ 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,\
/*  56 */ 0x02, 0x02, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,\
/*  64 */ 0x00, 0x00, 0x02, 0x4c, 0x4c, 0x00, 0x01, 0x01,\
/*  72 */ 0x05, 0x05, 0x15, 0x15, 0x15, 0x15, 0x15, 0x15,\
/*  80 */ 0x01, 0x4c, 0x4c, 0x4c, 0x4c, 0x4c, 0x4c, 0x4c,\
/*  88 */ 0x4c, 0x4c, 0x4c, 0x01, 0x24, 0x02, 0x01, 0x01,\
/*  96 */ 0x01, 0x00, 0x00, 0x02, 0x01, 0x01, 0x01, 0x01,\
/* 104 */ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0x14,\
/* 112 */ 0x04, 0x01, 0x02, 0x00, 0x01, 0x08, 0x05, 0x05,\
/* 120 */ 0x05, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,\
/* 128 */ 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,\
/* 136 */ 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x04, 0x04,\
/* 144 */ 0x04, 0x04, 0x00,}
