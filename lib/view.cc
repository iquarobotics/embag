#include "view.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <string>

#include "ros_message.h"
#include "ros_value.h"
#include "util.h"

namespace Embag {
View::iterator View::begin() {
  return iterator{this, iterator::begin_cond_t{}};
}

View::iterator View::end() {
  return iterator{this};
}

View::iterator::iterator(View *view, begin_cond_t begin_cond) : view_(view) {
  // Read a message from each bag into the corresponding bag wrapper
  for (auto &pair : view_->bag_wrappers_) {
    auto& wrapper = pair.second;
    wrapper->chunk_iter = wrapper->chunks_to_parse.begin();

    readMessage(wrapper);
  }
}

std::shared_ptr<RosMessage> View::iterator::operator*() const {
  // Take the first wrapper from the priority queue
  auto wrapper = msg_queue_.top();

  const auto &connection = wrapper->bag->connections_[wrapper->current_connection_id];
  const auto &msg_def = wrapper->bag->msgDefForTopic(connection.topic);

  auto message = std::make_shared<RosMessage>(wrapper->current_message_buffer, wrapper->current_message_data_offset);

  message->topic = connection.topic;
  message->timestamp = wrapper->current_timestamp;
  message->md5 = connection.data.md5sum;
  message->raw_data_len = wrapper->current_message_len;
  message->msg_def_ = msg_def;

  return message;
}

// This implementation of readHeader is faster but less flexible than the map-based version in Embag.
View::iterator::header_t View::iterator::readHeader(const RosBagTypes::record_t &record) {
  header_t header{};

  auto p = record.header;
  const char *end = record.header + record.header_len;

  while (p < end) {
    const uint32_t field_len = *(reinterpret_cast<const uint32_t *>(p));

    p += sizeof(uint32_t);

    const auto value_p = strstr(p, "=") + 1;

    if (value_p == nullptr) {
      throw std::runtime_error("Unable to find '=' in header field - perhaps this bag is corrupt...");
    }

    // Compare the first char here for optimization reasons since this code gets pretty hot
    if (*p == 'o') {        // op
      header.op = RosBagTypes::header_t::op(*value_p);
    } else if (*p == 'c') { // conn
      header.connection_id = *reinterpret_cast<const uint32_t *>(value_p);
    } else if (*p == 't') { // time
      header.timestamp = *reinterpret_cast<const RosValue::ros_time_t *>(value_p);
    }

    p += field_len;
  }

  return header;
}

/*
 * initialize: fill the queue with a message from each bag
 * store the message with the smallest timestamp in current_msg_ stuff and remove it from the queue
 * read another message from the bag that had its message removed from the queue
 */
void View::iterator::readMessage(std::shared_ptr<bag_wrapper_t> bag_wrapper) {
  while (bag_wrapper->chunk_iter != bag_wrapper->chunks_to_parse.end()) {
    if (!bag_wrapper->current_buffer) {
      const auto& chunk = *(bag_wrapper->chunk_iter);
      bag_wrapper->current_buffer = std::make_shared<std::vector<char>>(chunk->uncompressed_size);
      chunk->decompress(&bag_wrapper->current_buffer->at(0));
      bag_wrapper->uncompressed_size = chunk->uncompressed_size;
    }

    while (bag_wrapper->processed_bytes < bag_wrapper->uncompressed_size) {
      RosBagTypes::record_t record{};

      // TODO: just use pointers instead of copying memory?
      std::memcpy(&record.header_len,
                  &bag_wrapper->current_buffer->at(bag_wrapper->processed_bytes),
                  sizeof(record.header_len));
      bag_wrapper->processed_bytes += sizeof(record.header_len);
      record.header = &bag_wrapper->current_buffer->at(bag_wrapper->processed_bytes);
      bag_wrapper->processed_bytes += record.header_len;

      std::memcpy(&record.data_len,
                  &bag_wrapper->current_buffer->at(bag_wrapper->processed_bytes),
                  sizeof(record.data_len));
      bag_wrapper->processed_bytes += sizeof(record.data_len);
      record.data = &bag_wrapper->current_buffer->at(bag_wrapper->processed_bytes);
      bag_wrapper->processed_bytes += record.data_len;

      const auto header = readHeader(record);

      switch (header.op) {
        case RosBagTypes::header_t::op::MESSAGE_DATA: {
          // Check if this is a topic we're interested in
          if (bag_wrapper->connection_ids.count(header.connection_id) == 0) {
            continue;
          }

          bag_wrapper->current_message_buffer = bag_wrapper->current_buffer;
          bag_wrapper->current_message_data_offset = record.data - &bag_wrapper->current_message_buffer->at(0);
          bag_wrapper->current_message_len = record.data_len;
          bag_wrapper->current_connection_id = header.connection_id;
          bag_wrapper->current_timestamp = header.timestamp;

          msg_queue_.push(bag_wrapper);
          return;
        }
        case RosBagTypes::header_t::op::CONNECTION: {
          // TODO: not entirely sure what to do with these so we'll move to the next record...
          continue;
        }
        default: {
          throw std::runtime_error("Found unknown record type: " + std::to_string(static_cast<int>(header.op)));
        }
      }
    }

    bag_wrapper->chunk_iter++;
    bag_wrapper->current_buffer.reset();
    bag_wrapper->processed_bytes = 0;
  }
}

View::iterator &View::iterator::operator++() {
  auto wrapper = msg_queue_.top();
  msg_queue_.pop();

  readMessage(wrapper);

  return *this;
}

View View::getMessages() {
  bag_wrappers_.clear();

  for (size_t bag_index = 0; bag_index < bags_.size(); ++bag_index) {
    const auto& bag = bags_[bag_index];
    bag_wrappers_[bag] = std::make_shared<iterator::bag_wrapper_t>();
    bag_wrappers_[bag]->bag = bag;
    bag_wrappers_[bag]->bag_index = bag_index;

    for (const auto &chunk : bag->chunks_) {
      bag_wrappers_[bag]->chunks_to_parse.emplace(&chunk);
    }

    for (size_t i = 0; i < bag->connections_.size(); i++) {
      bag_wrappers_[bag]->connection_ids.emplace(i);
    }
  }

  return *this;
}

View View::getMessages(const std::string &topic) {
  return getMessages({topic});
}

View View::getMessages(const std::vector<std::string> &topics) {
  bag_wrappers_.clear();

  for (size_t bag_index = 0; bag_index < bags_.size(); ++bag_index) {
    const auto& bag = bags_[bag_index];
    bag_wrappers_[bag] = std::make_shared<iterator::bag_wrapper_t>();
    bag_wrappers_[bag]->bag = bag;
    bag_wrappers_[bag]->bag_index = bag_index;

    for (const auto &topic : topics) {
      if (!bag->topic_connection_map_.count(topic)) {
        continue;
      }

      for (const auto &connection_record : bag->topic_connection_map_.at(topic)) {
        for (const auto &block : connection_record->blocks) {
          bag_wrappers_[bag]->chunks_to_parse.emplace(block.into_chunk);
        }

        bag_wrappers_[bag]->connection_ids.emplace(connection_record->id);
      }
    }
  }

  return *this;
}

View View::getMessages(std::initializer_list<std::string> topics) {
  return getMessages(std::vector<std::string>(topics.begin(), topics.end()));
}

RosValue::ros_time_t View::getStartTime() {
  RosValue::ros_time_t start_time;
  start_time.secs = UINT32_MAX;
  start_time.nsecs = UINT32_MAX;

  for (const auto& bag : bags_) {
    if (bag->chunks_.empty()) {
      continue;
    }

    const auto& bag_start = bag->chunks_.front().info.start_time;
    if (bag_start.secs < start_time.secs) {
      start_time = bag_start;
    } else if (bag_start.secs == start_time.secs && bag_start.nsecs < start_time.nsecs) {
      start_time = bag_start;
    }
  }

  return start_time;
}

RosValue::ros_time_t View::getEndTime() {
  RosValue::ros_time_t end_time;
  end_time.secs = 0;
  end_time.nsecs = 0;

  for (const auto& bag : bags_) {
    if (bag->chunks_.empty()) {
      continue;
    }

    const auto& bag_end = bag->chunks_.back().info.end_time;
    if (bag_end.secs > end_time.secs) {
      end_time = bag_end;
    } else if (bag_end.secs == end_time.secs && bag_end.nsecs > end_time.nsecs) {
      end_time = bag_end;
    }
  }

  return end_time;
}

View View::addBag(const std::string &filename) {
  auto bag = std::make_shared<Embag::Bag>(filename);
  addBag(bag);
  return *this;
}

View View::addBag(std::shared_ptr<Bag> bag) {
  bags_.emplace_back(bag);
  message_index_cache_.clear();
  return *this;
}

const std::vector<View::message_ref_t>& View::getMessageIndex(const std::string& topic) {
  const auto it = message_index_cache_.find(topic);
  if (it != message_index_cache_.end()) {
    return it->second;
  }

  std::vector<message_ref_t> refs;
  for (size_t bag_index = 0; bag_index < bags_.size(); ++bag_index) {
    const auto& bag = bags_[bag_index];
    const auto conn_it = bag->topic_connection_map_.find(topic);
    if (conn_it == bag->topic_connection_map_.end()) {
      continue;
    }
    for (const auto* connection : conn_it->second) {
      for (const auto& block : connection->blocks) {
        const char* p = block.entries;
        for (uint32_t i = 0; i < block.message_count; ++i, p += RosBagTypes::index_block_t::ENTRY_SIZE) {
          message_ref_t ref{};
          std::memcpy(&ref.timestamp.secs, p, sizeof(uint32_t));
          std::memcpy(&ref.timestamp.nsecs, p + 4, sizeof(uint32_t));
          std::memcpy(&ref.offset, p + 8, sizeof(uint32_t));
          ref.connection = connection;
          ref.chunk = block.into_chunk;
          ref.bag_index = static_cast<uint32_t>(bag_index);
          refs.push_back(ref);
        }
      }
    }
  }

  // Same order as the iterator: timestamp, then bag insertion order, then position in the bag file.
  std::sort(refs.begin(), refs.end(), [](const message_ref_t& a, const message_ref_t& b) {
    if (a.timestamp.secs != b.timestamp.secs) {
      return a.timestamp.secs < b.timestamp.secs;
    }
    if (a.timestamp.nsecs != b.timestamp.nsecs) {
      return a.timestamp.nsecs < b.timestamp.nsecs;
    }
    if (a.bag_index != b.bag_index) {
      return a.bag_index < b.bag_index;
    }
    if (a.chunk->offset != b.chunk->offset) {
      return a.chunk->offset < b.chunk->offset;
    }
    return a.offset < b.offset;
  });

  return message_index_cache_.emplace(topic, std::move(refs)).first->second;
}

const View::message_ref_t& View::getMessageRef(const std::string& topic, size_t index) {
  const auto& refs = getMessageIndex(topic);
  if (index >= refs.size()) {
    throw std::out_of_range("Message index " + std::to_string(index) + " out of range for topic " + topic + " (" +
                            std::to_string(refs.size()) + " messages)");
  }
  return refs[index];
}

size_t View::getMessageCount(const std::string& topic) { return getMessageIndex(topic).size(); }

size_t View::getMessageBagIndex(const std::string& topic, size_t index) {
  return getMessageRef(topic, index).bag_index;
}

std::shared_ptr<RosMessage> View::getMessageByIndex(const std::string& topic, size_t index) {
  const auto& ref = getMessageRef(topic, index);
  const auto* chunk = ref.chunk;

  // Uncompressed chunks are read in place from the bag bytes; compressed ones are decompressed once and cached.
  const char* chunk_data = nullptr;
  uint64_t chunk_size = chunk->uncompressed_size;
  if (chunk->compression == "none") {
    chunk_data = chunk->record.data;
    chunk_size = std::min<uint64_t>(chunk_size, chunk->record.data_len);
  } else {
    if (chunk != cached_chunk_) {
      // Free the previous chunk before allocating the next one.
      cached_chunk_ = nullptr;
      cached_chunk_buffer_.reset();
      auto buffer = std::make_shared<std::vector<char>>(chunk->uncompressed_size);
      chunk->decompress(buffer->data());
      cached_chunk_buffer_ = std::move(buffer);
      cached_chunk_ = chunk;
    }
    chunk_data = cached_chunk_buffer_->data();
  }

  // MESSAGE_DATA record layout: header_len (4) | header | data_len (4) | data
  const auto require = [&](uint64_t end) {
    if (end > chunk_size) {
      throw std::runtime_error("Message record out of chunk bounds for topic " + topic +
                               ", perhaps this bag is corrupt...");
    }
  };
  uint64_t pos = ref.offset;
  uint32_t header_len = 0;
  require(pos + sizeof(header_len));
  std::memcpy(&header_len, chunk_data + pos, sizeof(header_len));
  pos += sizeof(header_len) + header_len;
  uint32_t data_len = 0;
  require(pos + sizeof(data_len));
  std::memcpy(&data_len, chunk_data + pos, sizeof(data_len));
  pos += sizeof(data_len);
  require(pos + data_len);

  std::shared_ptr<std::vector<char>> buffer;
  size_t data_offset = 0;
  if (chunk->compression == "none") {
    // Copy only this message, so the returned message does not keep a whole chunk alive.
    buffer = std::make_shared<std::vector<char>>(chunk_data + pos, chunk_data + pos + data_len);
  } else {
    buffer = cached_chunk_buffer_;
    data_offset = pos;
  }

  const auto& bag = bags_[ref.bag_index];
  return std::make_shared<RosMessage>(ref.connection->topic, ref.timestamp, ref.connection->data.md5sum, buffer,
                                      data_offset, data_len, bag->msgDefForTopic(ref.connection->topic));
}
}  // namespace Embag
