/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "internal_fwd_decl.h"
#include "txn_cursor.h"
#include "db.h"
#include "env.h"
#include "mem.h"

ham_bool_t
txn_cursor_is_nil(txn_cursor_t *cursor)
{
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED)
        return (HAM_FALSE);
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_UNCOUPLED)
        return (HAM_FALSE);
    return (HAM_TRUE);
}

void
txn_cursor_set_to_nil(txn_cursor_t *cursor)
{
    ham_env_t *env=db_get_env(txn_cursor_get_db(cursor));

    /* uncoupled cursor? free the cached pointer */
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_UNCOUPLED) {
        ham_key_t *key=txn_cursor_get_uncoupled_key(cursor);
        if (key) {
            if (key->data)
                allocator_free(env_get_allocator(env), key->data);
            allocator_free(env_get_allocator(env), key);
        }
        txn_cursor_set_uncoupled_key(cursor, 0);
        txn_cursor_set_flags(cursor, 
                txn_cursor_get_flags(cursor)&(~TXN_CURSOR_FLAG_UNCOUPLED));
    }
    /* uncoupled cursor? remove from the txn_op structure */
    else if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        if (op)
            txn_op_remove_cursor(op, cursor);
        txn_cursor_set_flags(cursor, 
                txn_cursor_get_flags(cursor)&(~TXN_CURSOR_FLAG_COUPLED));
    }

    /* otherwise cursor is already nil */
}

txn_cursor_t *
txn_cursor_clone(txn_cursor_t *cursor)
{
    return (0);
}

void
txn_cursor_close(txn_cursor_t *cursor)
{
}

void
txn_cursor_overwrite(txn_cursor_t *cursor, ham_record_t *record)
{
}

ham_status_t
txn_cursor_move(txn_cursor_t *cursor, ham_u32_t flags)
{
    return (0);
}

ham_status_t
txn_cursor_find(txn_cursor_t *cursor, ham_key_t *key)
{
    return (0);
}

ham_status_t
txn_cursor_insert(txn_cursor_t *cursor, ham_key_t *key, ham_record_t *record,
                ham_u32_t flags)
{
    return (0);
}

ham_status_t
txn_cursor_get_key(txn_cursor_t *cursor, ham_key_t *key)
{
    ham_db_t *db=txn_cursor_get_db(cursor);
    ham_key_t *source=0;

    /* coupled cursor? get key from the txn_op structure */
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        txn_opnode_t *node=txn_op_get_node(op);

        ham_assert(db==txn_opnode_get_db(node), (""));
        source=txn_opnode_get_key(node);

        key->size=source->size;
        if (source->data && source->size) {
            if (!(key->flags&HAM_KEY_USER_ALLOC)) {
                ham_status_t st=db_resize_key_allocdata(db, source->size);
                if (st)
                    return (st);
                key->data=db_get_key_allocdata(db);
            }
            memcpy(key->data, source->data, source->size);
        }
        else
            key->data=0;

    }
    /* 
     * uncoupled cursor? then the cursor was flushed to the btree. return
     * HAM_INTERNAL_ERROR to force the caller to lookup the btree. 
     */
    else if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_UNCOUPLED)
        return (HAM_INTERNAL_ERROR);
    /* 
     * otherwise cursor is nil and we cannot return a key 
     */
    else
        return (HAM_CURSOR_IS_NIL);

    return (0);
}

ham_status_t
txn_cursor_get_record(txn_cursor_t *cursor, ham_record_t *record)
{
    ham_db_t *db=txn_cursor_get_db(cursor);
    ham_record_t *source=0;

    /* coupled cursor? get record from the txn_op structure */
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED) {
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        source=txn_op_get_record(op);

        record->size=source->size;
        if (source->data && source->size) {
            if (!(record->flags&HAM_RECORD_USER_ALLOC)) {
                ham_status_t st=db_resize_record_allocdata(db, source->size);
                if (st)
                    return (st);
                record->data=db_get_record_allocdata(db);
            }
            memcpy(record->data, source->data, source->size);
        }
        else
            record->data=0;
    }
    /* 
     * uncoupled cursor? then the cursor was flushed to the btree. return
     * HAM_INTERNAL_ERROR to force the caller to lookup the btree. 
     */
    else if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_UNCOUPLED)
        return (HAM_INTERNAL_ERROR);
    /* 
     * otherwise cursor is nil and we cannot return a key 
     */
    else
        return (HAM_CURSOR_IS_NIL);

    return (0);
}

ham_status_t
txn_cursor_erase(txn_cursor_t *cursor, ham_key_t *key)
{
    return (0);
}

ham_status_t
txn_cursor_get_duplicate_count(txn_cursor_t *cursor, ham_u32_t *count)
{
    return (0);
}
