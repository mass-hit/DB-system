#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  ++current_reads_[read_ts];
  watermark_=std::min(watermark_,read_ts);
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  --current_reads_[read_ts];
  if(current_reads_[read_ts]==0) {
    current_reads_.erase(read_ts);
    if(watermark_==read_ts) {
      if(current_reads_.empty()) {
        watermark_=commit_ts_;
      }else {
        watermark_=current_reads_.begin()->first;
      }
    }
  }
}

}  // namespace bustub
