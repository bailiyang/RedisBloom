#include <math.h>    // ceil, log10f
#include <stdlib.h>  // malloc
#include <strings.h> // strncasecmp

#include "tdigest.h"
#include "rmutil/util.h"
#include "version.h"

#include "rm_tdigest.h"

RedisModuleType *TDigestSketchType;

int TDigestSketch_Create(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    long long compression = 0;
    RedisModuleString *keyName = argv[1];
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != TDigestSketchType) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    if (RedisModule_StringToLongLong(argv[2], &compression) != REDISMODULE_OK) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, "ERR T-Digest: error parsing compression parameter");
    }
    if (compression <= 0) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(
            ctx, "ERR T-Digest: compression parameter needs to be a positive integer");
    }
    td_histogram_t *tdigest = td_new(compression);
    RedisModule_ModuleTypeSetValue(key, TDigestSketchType, tdigest);
    RedisModule_CloseKey(key);
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int TDigestSketch_Add(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModuleString *keyName = argv[1];
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, "ERR T-Digest: key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != TDigestSketchType) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    td_histogram_t *tdigest = RedisModule_ModuleTypeGetValue(key);
    double val = 0.0, weight = 0.0;
    for (int i = 2; i < argc; i += 2) {
        if (RedisModule_StringToDouble(argv[i], &val) != REDISMODULE_OK) {
            RedisModule_CloseKey(key);
            return RedisModule_ReplyWithError(ctx, "ERR T-Digest: error parsing val parameter");
        }
        if (RedisModule_StringToDouble(argv[i + 1], &val) != REDISMODULE_OK) {
            RedisModule_CloseKey(key);
            return RedisModule_ReplyWithError(ctx, "ERR T-Digest: error parsing weight parameter");
        }
        td_add(tdigest, val, weight);
    }
    RedisModule_CloseKey(key);
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

void TDigestRdbSave(RedisModuleIO *io, void *obj) {}

void *TDigestRdbLoad(RedisModuleIO *io, int encver) {}

void TDigestFree(void *value) {}

size_t TDigestMemUsage(const void *value) {
    td_histogram_t *tdigest = (td_histogram_t *)value;
    return sizeof(tdigest);
}

int TDigestModule_onLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // TODO: add option to set defaults from command line and in program
    RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                                 .rdb_load = TDigestRdbLoad,
                                 .rdb_save = TDigestRdbSave,
                                 .aof_rewrite = RMUtil_DefaultAofRewrite,
                                 .mem_usage = TDigestMemUsage,
                                 .free = TDigestFree};

    TDigestSketchType = RedisModule_CreateDataType(ctx, "TDIS-TYPE", TDIGEST_ENC_VER, &tm);
    if (TDigestSketchType == NULL)
        return REDISMODULE_ERR;

    RMUtil_RegisterWriteDenyOOMCmd(ctx, "tdigest.create", TDigestSketch_Create);
    RMUtil_RegisterWriteDenyOOMCmd(ctx, "tdigest.add", TDigestSketch_Add);
    return REDISMODULE_OK;
}