/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#include "0root/root.h"

#include <string.h>
#ifndef UPS_OS_WIN32
#  include <libgen.h>
#endif

#include "1base/error.h"
#include "1errorinducer/errorinducer.h"
#include "1os/os.h"
#include "2device/device.h"
#include "2compressor/compressor_factory.h"
#include "3journal/journal.h"
#include "3page_manager/page_manager.h"
#include "4db/db.h"
#include "4txn/txn_local.h"
#include "4env/env_local.h"
#include "4context/context.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

Journal::Journal(LocalEnvironment *env)
  : env_(env), current_fd_(0),
    threshold_(env->config().journal_switch_threshold), disable_logging_(false),
    count_bytes_flushed_(0), count_bytes_before_compression_(0),
    count_bytes_after_compression_(0)
{
  if (threshold_ == 0)
    threshold_ = kSwitchTxnThreshold;

  open_txn_[0] = 0;
  open_txn_[1] = 0;
  closed_txn_[0] = 0;
  closed_txn_[1] = 0;

  int algo = env_->config().journal_compressor;
  if (algo)
    compressor_.reset(CompressorFactory::create(algo));
}

void
Journal::create()
{
  // create the two files
  for (int i = 0; i < 2; i++) {
    std::string path = get_path(i);
    files_[i].create(path.c_str(), 0644);
  }
}

void
Journal::open()
{
  // open the two files
  try {
    std::string path = get_path(0);
    files_[0].open(path.c_str(), false);
    path = get_path(1);
    files_[1].open(path.c_str(), 0);
  }
  catch (Exception &ex) {
    files_[1].close();
    files_[0].close();
    throw ex;
  }
}

int
Journal::switch_files_maybe()
{
  int other = current_fd_ ? 0 : 1;

  // determine the journal file which is used for this transaction 
  // if the "current" file is not yet full, continue to write to this file
  if (open_txn_[current_fd_] + closed_txn_[current_fd_] < threshold_)
    return current_fd_;

  // If the other file does no longer have open Transactions then
  // delete the other file and use the other file as the current file
  if (open_txn_[other] == 0) {
    clear_file(other);
    current_fd_ = other;
    // fall through
  }

  // Otherwise just continue using the current file
  return current_fd_;
}

void
Journal::append_txn_begin(LocalTransaction *txn, const char *name, uint64_t lsn)
{
  if (unlikely(disable_logging_))
    return;

  assert(notset(txn->get_flags(), UPS_TXN_TEMPORARY));

  PJournalEntry entry;
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeTxnBegin;
  entry.lsn = lsn;
  if (name)
    entry.followup_size = ::strlen(name) + 1;

  txn->set_log_desc(switch_files_maybe());

  int cur = txn->get_log_desc();

  if (txn->get_name().size())
    append_entry(cur, (uint8_t *)&entry, (uint32_t)sizeof(entry),
                (uint8_t *)txn->get_name().c_str(),
                (uint32_t)txn->get_name().size() + 1);
  else
    append_entry(cur, (uint8_t *)&entry, (uint32_t)sizeof(entry));
  maybe_flush_buffer(cur);

  open_txn_[cur]++;

  // store the fp-index in the journal structure; it's needed for
  // journal_append_checkpoint() to quickly find out which file is
  // the newest
  current_fd_ = cur;
}

void
Journal::append_txn_abort(LocalTransaction *txn, uint64_t lsn)
{
  if (unlikely(disable_logging_))
    return;

  assert(notset(txn->get_flags(), UPS_TXN_TEMPORARY));

  int idx;
  PJournalEntry entry;
  entry.lsn = lsn;
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeTxnAbort;

  // update the transaction counters of this logfile
  idx = txn->get_log_desc();
  open_txn_[idx]--;
  closed_txn_[idx]++;

  append_entry(idx, (uint8_t *)&entry, sizeof(entry));
  maybe_flush_buffer(idx);
  // no need for fsync - incomplete transactions will be aborted anyway
}

void
Journal::append_txn_commit(LocalTransaction *txn, uint64_t lsn)
{
  if (unlikely(disable_logging_))
    return;

  assert(notset(txn->get_flags(), UPS_TXN_TEMPORARY));

  PJournalEntry entry;
  entry.lsn = lsn;
  entry.txn_id = txn->get_id();
  entry.type = kEntryTypeTxnCommit;

  // do not yet update the transaction counters of this logfile; just
  // because the txn was committed does not mean that it will be flushed
  // immediately. The counters will be modified in transaction_flushed().
  int idx = txn->get_log_desc();

  append_entry(idx, (uint8_t *)&entry, sizeof(entry));

  // and flush the file
  flush_buffer(idx, isset(env_->get_flags(), UPS_ENABLE_FSYNC));
}

void
Journal::append_insert(Database *db, LocalTransaction *txn,
                ups_key_t *key, ups_record_t *record, uint32_t flags,
                uint64_t lsn)
{
  if (unlikely(disable_logging_))
    return;

  PJournalEntry entry;

  entry.lsn = lsn;
  entry.dbname = db->name();
  entry.type = kEntryTypeInsert;
  // the followup_size will be filled in later when we know whether
  // compression is used
  entry.followup_size = sizeof(PJournalEntryInsert) - 1;

  int idx;
  if (isset(txn->get_flags(), UPS_TXN_TEMPORARY)) {
    entry.txn_id = 0;
    idx = switch_files_maybe();
    closed_txn_[idx]++;
  }
  else {
    entry.txn_id = txn->get_id();
    idx = txn->get_log_desc();
  }

  PJournalEntryInsert insert;
  insert.key_size = key->size;
  insert.record_size = record->size;
  insert.insert_flags = flags;

  // we need the current position in the file buffer. if compression is enabled
  // then we do not know the actual followup-size of this entry. it will be
  // patched in later.
  uint32_t entry_position = buffer_[idx].size();

  // write the header information
  append_entry(idx, (uint8_t *)&entry, sizeof(entry),
              (uint8_t *)&insert, sizeof(PJournalEntryInsert) - 1);

  // try to compress the payload; if the compressed result is smaller than
  // the original (uncompressed) payload then use it
  const void *key_data = key->data;
  uint32_t key_size = key->size;
  if (compressor_.get()) {
    count_bytes_before_compression_ += key_size;
    uint32_t len = compressor_->compress((uint8_t *)key->data, key->size);
    if (len < key->size) {
      key_size = len;
      key_data = compressor_->arena.data();
      insert.compressed_key_size = len;
    }
    count_bytes_after_compression_ += key_size;
  }
  append_entry(idx, (uint8_t *)key_data, key_size);
  entry.followup_size += key_size;

  // and now the same for the record data
  const void *record_data = record->data;
  uint32_t record_size = record->size;
  if (compressor_.get()) {
    count_bytes_before_compression_ += record_size;
    uint32_t len = compressor_->compress((uint8_t *)record->data, record_size);
    if (len < record_size) {
      record_size = len;
      record_data = compressor_->arena.data();
      insert.compressed_record_size = len;
    }
    count_bytes_after_compression_ += record_size;
  }
  append_entry(idx, (uint8_t *)record_data, record_size);
  entry.followup_size += record_size;

  // now overwrite the patched entry
  buffer_[idx].overwrite(entry_position,
                  (uint8_t *)&entry, sizeof(entry));
  buffer_[idx].overwrite(entry_position + sizeof(entry),
                  (uint8_t *)&insert, sizeof(PJournalEntryInsert) - 1);

  maybe_flush_buffer(idx);
}

void
Journal::append_erase(Database *db, LocalTransaction *txn, ups_key_t *key,
                int duplicate_index, uint32_t flags, uint64_t lsn)
{
  if (unlikely(disable_logging_))
    return;

  PJournalEntry entry;
  PJournalEntryErase erase;
  const void *payload_data = key->data;
  uint32_t payload_size = key->size;

  // try to compress the payload; if the compressed result is smaller than
  // the original (uncompressed) payload then use it
  if (compressor_.get()) {
    count_bytes_before_compression_ += payload_size;
    uint32_t len = compressor_->compress((uint8_t *)key->data, key->size);
    if (len < key->size) {
      payload_data = compressor_->arena.data();
      payload_size = len;
      erase.compressed_key_size = len;
    }
    count_bytes_after_compression_ += payload_size;
  }

  entry.lsn = lsn;
  entry.dbname = db->name();
  entry.type = kEntryTypeErase;
  entry.followup_size = sizeof(PJournalEntryErase) + payload_size - 1;
  erase.key_size = key->size;
  erase.erase_flags = flags;
  erase.duplicate = duplicate_index;

  int idx;
  if (isset(txn->get_flags(), UPS_TXN_TEMPORARY)) {
    entry.txn_id = 0;
    idx = switch_files_maybe();
    closed_txn_[idx]++;
  }
  else {
    entry.txn_id = txn->get_id();
    idx = txn->get_log_desc();
  }

  // append the entry to the logfile
  append_entry(idx, (uint8_t *)&entry, sizeof(entry),
                (uint8_t *)&erase, sizeof(PJournalEntryErase) - 1,
                (uint8_t *)payload_data, payload_size);
  maybe_flush_buffer(idx);
}

int
Journal::append_changeset(std::vector<Page *> &pages,
                uint64_t last_blob_page, uint64_t lsn)
{
  assert(pages.size() > 0);

  if (unlikely(disable_logging_))
    return -1;

  (void)switch_files_maybe();

  PJournalEntry entry;
  PJournalEntryChangeset changeset;
  
  entry.lsn = lsn;
  entry.dbname = 0;
  entry.txn_id = 0;
  entry.type = kEntryTypeChangeset;
  // followup_size is incomplete - the actual page sizes are added later
  entry.followup_size = sizeof(PJournalEntryChangeset);
  changeset.num_pages = pages.size();
  changeset.last_blob_page = last_blob_page;

  // we need the current position in the file buffer. if compression is enabled
  // then we do not know the actual followup-size of this entry. it will be
  // patched in later.
  uint32_t entry_position = buffer_[current_fd_].size();

  // write the data to the file
  append_entry(current_fd_, (uint8_t *)&entry, sizeof(entry),
                (uint8_t *)&changeset, sizeof(PJournalEntryChangeset));

  size_t page_size = env_->config().page_size_bytes;
  for (std::vector<Page *>::iterator it = pages.begin();
                  it != pages.end();
                  ++it) {
    entry.followup_size += append_changeset_page(*it, page_size);
  }

  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // and patch in the followup-size
  buffer_[current_fd_].overwrite(entry_position,
          (uint8_t *)&entry, sizeof(entry));

  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // and flush the file
  flush_buffer(current_fd_, isset(env_->get_flags(), UPS_ENABLE_FSYNC));

  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // if recovery is enabled (w/o transactions) then simulate a "commit" to
  // make sure that the log files are switched properly. Here, the
  // counter for "opened transactions" is incremented. It will be decremented
  // by the worker thread as soon as the dirty pages are flushed to disk.
  open_txn_[current_fd_]++;
  return current_fd_;
}

uint32_t
Journal::append_changeset_page(Page *page, uint32_t page_size)
{
  PJournalEntryPageHeader header(page->address());

  if (compressor_.get()) {
    count_bytes_before_compression_ += page_size;
    header.compressed_size = compressor_->compress((uint8_t *)page->data(),
                    page_size);
    append_entry(current_fd_, (uint8_t *)&header, sizeof(header),
                    compressor_->arena.data(),
                    header.compressed_size);
    count_bytes_after_compression_ += header.compressed_size;
    return header.compressed_size + sizeof(header);
  }

  append_entry(current_fd_, (uint8_t *)&header, sizeof(header),
                (uint8_t *)page->data(), page_size);
  return page_size + sizeof(header);
}

void
Journal::changeset_flushed(int fd_index)
{
  closed_txn_[fd_index]++;
}

void
Journal::transaction_flushed(LocalTransaction *txn)
{
  assert(notset(txn->get_flags(), UPS_TXN_TEMPORARY));
  if (unlikely(disable_logging_))
    return;

  int idx = txn->get_log_desc();
  assert(open_txn_[idx] > 0);
  open_txn_[idx]--;
  closed_txn_[idx]++;
}

void
Journal::read_entry(Iterator *iter, PJournalEntry *entry, ByteArray *auxbuffer)
{
  auxbuffer->clear();

  // if iter->offset is 0, then the iterator was created from scratch
  // and we start reading from the first (oldest) entry.
  //
  // The oldest of the two logfiles is always the "other" one (the one
  // NOT in current_fd).
  if (iter->offset == 0) {
    iter->fdstart = iter->fdidx =
                        current_fd_ == 0
                            ? 1
                            : 0;
  }

  // get the size of the journal file
  uint64_t filesize = files_[iter->fdidx].file_size();

  // reached EOF? then either skip to the next file or we're done
  if (filesize == iter->offset) {
    if (iter->fdstart == iter->fdidx) {
      iter->fdidx = iter->fdidx == 1 ? 0 : 1;
      iter->offset = 0;
      filesize = files_[iter->fdidx].file_size();
    }
    else {
      entry->lsn = 0;
      return;
    }
  }

  // second file is also empty? then return
  if (filesize == iter->offset) {
    entry->lsn = 0;
    return;
  }

  // now try to read the next entry
  try {
    files_[iter->fdidx].pread(iter->offset, entry, sizeof(*entry));

    iter->offset += sizeof(*entry);

    // read auxiliary data if it's available
    if (entry->followup_size) {
      auxbuffer->resize((uint32_t)entry->followup_size);

      files_[iter->fdidx].pread(iter->offset, auxbuffer->data(),
                      (size_t)entry->followup_size);
      iter->offset += entry->followup_size;
    }
  }
  catch (Exception &) {
    ups_trace(("failed to read journal entry, aborting recovery"));
    entry->lsn = 0; // this triggers the end of recovery
  }
}

void
Journal::close(bool noclear)
{
  // the noclear flag is set during testing, for checking whether the files
  // contain the correct data. Flush the buffers, otherwise the tests will
  // fail because data is missing
  if (noclear) {
    flush_buffer(0);
    flush_buffer(1);
  }

  if (!noclear)
    clear();

  for (int i = 0; i < 2; i++) {
    files_[i].close();
    buffer_[i].clear();
  }
}

Database *
Journal::get_db(uint16_t dbname)
{
  // first check if the Database is already open
  DatabaseMap::iterator it = database_map_.find(dbname);
  if (it != database_map_.end())
    return it->second;

  // not found - open it
  Database *db = 0;
  DbConfig config;
  config.db_name = dbname;
  ups_status_t st = env_->open_db(&db, config, 0);
  if (st)
    throw Exception(st);
  database_map_[dbname] = db;
  return db;
}

Transaction *
Journal::get_txn(LocalTransactionManager *txn_manager, uint64_t txn_id)
{
  Transaction *txn = txn_manager->get_oldest_txn();
  while (txn) {
    if (txn->get_id() == txn_id)
      return txn;
    txn = txn->get_next();
  }

  return 0;
}

void
Journal::close_all_databases()
{
  DatabaseMap::iterator it = database_map_.begin();
  while (it != database_map_.end()) {
    DatabaseMap::iterator it2 = it;
    it++;

    ups_status_t st = ups_db_close((ups_db_t *)it2->second, UPS_DONT_LOCK);
    if (st) {
      ups_log(("ups_db_close() failed w/ error %d (%s)", st, ups_strerror(st)));
      throw Exception(st);
    }
  }
  database_map_.clear();
}

void
Journal::abort_uncommitted_txns(LocalTransactionManager *txn_manager)
{
  Transaction *txn = txn_manager->get_oldest_txn();

  while (txn) {
    if (!txn->is_committed())
      txn->abort();
    txn = txn->get_next();
  }
}

void
Journal::recover(LocalTransactionManager *txn_manager)
{
  Context context(env_, 0, 0);

  // first redo the changesets
  uint64_t start_lsn = recover_changeset();

  // load the state of the PageManager; the PageManager state is loaded AFTER
  // physical recovery because its page might have been restored in
  // recover_changeset()
  uint64_t page_manager_blobid = env_->header()->page_manager_blobid();
  if (page_manager_blobid != 0)
    env_->page_manager()->initialize(page_manager_blobid);

  // then start the normal recovery
  if (isset(env_->get_flags(), UPS_ENABLE_TRANSACTIONS))
    recover_journal(&context, txn_manager, start_lsn);

  // clear the journal files
  clear();
}

uint64_t 
Journal::scan_for_oldest_changeset(File *file)
{
  Iterator it;
  PJournalEntry entry;
  ByteArray buffer;

  // get the next entry
  try {
    uint64_t filesize = file->file_size();

    while (it.offset < filesize) {
      file->pread(it.offset, &entry, sizeof(entry));

      if (entry.lsn == 0)
        break;

      if (entry.type == kEntryTypeChangeset) {
        return entry.lsn;
      }

      // increment the offset
      it.offset += sizeof(entry) + entry.followup_size;
    }
  }
  catch (Exception &ex) {
    ups_log(("exception (error %d) while reading journal", ex.code));
  }

  return 0;
}

uint64_t 
Journal::recover_changeset()
{
  // scan through both files, look for the file with the oldest changeset.
  uint64_t lsn1 = scan_for_oldest_changeset(&files_[0]);
  uint64_t lsn2 = scan_for_oldest_changeset(&files_[1]);

  // both files are empty or do not contain a changeset?
  if (lsn1 == 0 && lsn2 == 0)
    return 0;

  // now redo all changesets chronologically
  current_fd_ = lsn1 < lsn2 ? 0 : 1;

  uint64_t max_lsn1 = redo_all_changesets(current_fd_);
  uint64_t max_lsn2 = redo_all_changesets(current_fd_ == 0 ? 1 : 0);

  // return the lsn of the newest changeset
  return std::max(max_lsn1, max_lsn2);
}

uint64_t
Journal::redo_all_changesets(int fdidx)
{
  Iterator it;
  PJournalEntry entry;
  ByteArray buffer;
  uint64_t max_lsn = 0;

  // for each entry...
  try {
    uint64_t log_file_size = files_[fdidx].file_size();

    while (it.offset < log_file_size) {
      files_[fdidx].pread(it.offset, &entry, sizeof(entry));

      // Skip all log entries which are NOT from a changeset
      if (entry.type != kEntryTypeChangeset) {
        it.offset += sizeof(entry) + entry.followup_size;
        continue;
      }

      max_lsn = entry.lsn;

      it.offset += sizeof(entry);

      // Read the Changeset header
      PJournalEntryChangeset changeset;
      files_[fdidx].pread(it.offset, &changeset, sizeof(changeset));
      it.offset += sizeof(changeset);

      uint32_t page_size = env_->config().page_size_bytes;
      ByteArray arena(page_size);
      ByteArray tmp;

      uint64_t file_size = env_->device()->file_size();

      env_->page_manager()->set_last_blob_page_id(changeset.last_blob_page);

      // for each page in this changeset...
      for (uint32_t i = 0; i < changeset.num_pages; i++) {
        PJournalEntryPageHeader page_header;
        files_[fdidx].pread(it.offset, &page_header,
                        sizeof(page_header));
        it.offset += sizeof(page_header);
        if (page_header.compressed_size > 0) {
          tmp.resize(page_size);
          files_[fdidx].pread(it.offset, tmp.data(),
                        page_header.compressed_size);
          it.offset += page_header.compressed_size;
          compressor_->decompress(tmp.data(),
                        page_header.compressed_size, page_size, &arena);
        }
        else {
          files_[fdidx].pread(it.offset, arena.data(), page_size);
          it.offset += page_size;
        }

        Page *page;

        // now write the page to disk
        if (page_header.address == file_size) {
          file_size += page_size;

          page = new Page(env_->device());
          page->alloc(0);
        }
        else if (page_header.address > file_size) {
          file_size = (size_t)page_header.address + page_size;
          env_->device()->truncate(file_size);

          page = new Page(env_->device());
          page->fetch(page_header.address);
        }
        else {
          if (page_header.address == 0)
            page = env_->header()->header_page();
          else
            page = new Page(env_->device());
          page->fetch(page_header.address);
        }
        assert(page->address() == page_header.address);

        // overwrite the page data
        ::memcpy(page->data(), arena.data(), page_size);

        // flush the modified page to disk
        page->set_dirty(true);
        page->flush();

        if (page_header.address != 0)
          delete page;
      }
    }
  }
  catch (Exception &) {
    ups_trace(("Exception when applying changeset"));
    // propagate error
    throw;
  }

  return max_lsn;
}

void
Journal::recover_journal(Context *context,
                LocalTransactionManager *txn_manager, uint64_t start_lsn)
{
  ups_status_t st = 0;
  Iterator it;
  ByteArray buffer;

  /* recovering the journal is rather simple - we iterate over the
   * files and re-apply EVERY operation (incl. txn_begin and txn_abort),
   * that was not yet flushed with a Changeset.
   *
   * Basically we iterate over both log files and skip everything with
   * a sequence number (lsn) smaller the one of the last Changeset.
   *
   * When done then auto-abort all transactions that were not yet
   * committed.
   */

  // make sure that there are no pending transactions - start with
  // a clean state!
  assert(txn_manager->get_oldest_txn() == 0);
  assert(isset(env_->get_flags(), UPS_ENABLE_TRANSACTIONS));

  // do not append to the journal during recovery
  disable_logging_ = true;

  do {
    PJournalEntry entry;

    // get the next entry
    read_entry(&it, &entry, &buffer);

    // reached end of logfile?
    if (!entry.lsn)
      break;

    // re-apply this operation
    switch (entry.type) {
      case kEntryTypeTxnBegin: {
        Transaction *txn = 0;
        st = ups_txn_begin((ups_txn_t **)&txn, (ups_env_t *)env_, 
                (const char *)buffer.data(), 0, UPS_DONT_LOCK);
        // on success: patch the txn ID
        if (st == 0) {
          txn->set_id(entry.txn_id);
          txn_manager->set_txn_id(entry.txn_id);
        }
        break;
      }
      case kEntryTypeTxnAbort: {
        Transaction *txn = get_txn(txn_manager, entry.txn_id);
        st = ups_txn_abort((ups_txn_t *)txn, UPS_DONT_LOCK);
        break;
      }
      case kEntryTypeTxnCommit: {
        Transaction *txn = get_txn(txn_manager, entry.txn_id);
        st = ups_txn_commit((ups_txn_t *)txn, UPS_DONT_LOCK);
        break;
      }
      case kEntryTypeInsert: {
        PJournalEntryInsert *ins = (PJournalEntryInsert *)buffer.data();
        Transaction *txn = 0;
        Database *db;
        ups_key_t key = {0};
        ups_record_t record = {0};
        if (!ins) {
          st = UPS_IO_ERROR;
          goto bail;
        }

        // do not insert if the key was already flushed to disk
        if (entry.lsn <= start_lsn)
          continue;

        uint8_t *payload = ins->key_data();

        // extract the key - it can be compressed or uncompressed
        ByteArray keyarena;
        if (ins->compressed_key_size != 0) {
          compressor_->decompress(payload, ins->compressed_key_size,
                          ins->key_size);
          keyarena.append(compressor_->arena.data(), ins->key_size);
          key.data = keyarena.data();
          payload += ins->compressed_key_size;
        }
        else {
          key.data = payload;
          payload += ins->key_size;
        }
        key.size = ins->key_size;
        // extract the record - it can be compressed or uncompressed
        ByteArray recarena;
        if (ins->compressed_record_size != 0) {
          compressor_->decompress(payload, ins->compressed_record_size,
                          ins->record_size);
          recarena.append(compressor_->arena.data(), ins->record_size);
          record.data = recarena.data();
          payload += ins->compressed_record_size;
        }
        else {
          record.data = payload;
          payload += ins->record_size;
        }
        record.size = ins->record_size;
        if (entry.txn_id)
          txn = get_txn(txn_manager, entry.txn_id);
        db = get_db(entry.dbname);
        st = ups_db_insert((ups_db_t *)db, (ups_txn_t *)txn, &key, &record,
                        ins->insert_flags | UPS_DONT_LOCK);
        break;
      }
      case kEntryTypeErase: {
        PJournalEntryErase *e = (PJournalEntryErase *)buffer.data();
        Transaction *txn = 0;
        Database *db;
        ups_key_t key = {0};
        if (!e) {
          st = UPS_IO_ERROR;
          goto bail;
        }

        // do not erase if the key was already erased from disk
        if (entry.lsn <= start_lsn)
          continue;

        if (entry.txn_id)
          txn = get_txn(txn_manager, entry.txn_id);
        db = get_db(entry.dbname);
        key.data = e->key_data();
        if (e->compressed_key_size != 0) {
          compressor_->decompress(e->key_data(), e->compressed_key_size,
                          e->key_size);
          key.data = compressor_->arena.data();
        }
        else
          key.data = e->key_data();
        key.size = e->key_size;
        st = ups_db_erase((ups_db_t *)db, (ups_txn_t *)txn, &key,
                        e->erase_flags | UPS_DONT_LOCK);
        // key might have already been erased when the changeset
        // was flushed
        if (st == UPS_KEY_NOT_FOUND)
          st = 0;
        break;
      }
      case kEntryTypeChangeset: {
        // skip this; the changeset was already applied
        break;
      }
      default:
        ups_log(("invalid journal entry type or journal is corrupt"));
        st = UPS_IO_ERROR;
      }

      if (st)
        goto bail;
  } while (1);

bail:
  // all transactions which are not yet committed will be aborted
  abort_uncommitted_txns(txn_manager);

  // also close and delete all open databases - they were created in get_db()
  close_all_databases();

  // flush all committed transactions
  if (st == 0)
    st = env_->flush(UPS_FLUSH_COMMITTED_TRANSACTIONS);

  // re-enable the logging
  disable_logging_ = false;

  if (st)
    throw Exception(st);
}

void
Journal::clear_file(int idx)
{
  if (files_[idx].is_open()) {
    files_[idx].truncate(0);

    // after truncate, the file pointer is far beyond the new end of file;
    // reset the file pointer, or the next write will resize the file to
    // the original size
    files_[idx].seek(0, File::kSeekSet);
  }

  // clear the transaction counters
  open_txn_[idx] = 0;
  closed_txn_[idx] = 0;

  // also clear the buffer with the outstanding data
  buffer_[idx].clear();
}

std::string
Journal::get_path(int i)
{
  std::string path;

  if (env_->config().log_filename.empty()) {
    path = env_->config().filename;
  }
  else {
    path = env_->config().log_filename;
#ifdef UPS_OS_WIN32
    path += "\\";
    char fname[_MAX_FNAME];
    char ext[_MAX_EXT];
    _splitpath(env_->config().filename.c_str(), 0, 0, fname, ext);
    path += fname;
    path += ext;
#else
    path += "/";
    path += ::basename((char *)env_->config().filename.c_str());
#endif
  }
  if (i == 0)
    path += ".jrn0";
  else if (i == 1)
    path += ".jrn1";
  else
    assert(!"invalid index");
  return (path);
}

} // namespace upscaledb
