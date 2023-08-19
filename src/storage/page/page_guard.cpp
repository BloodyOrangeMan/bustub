#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    if (is_dirty_) {
      bpm_->UnpinPage(page_->GetPageId(), true);
    } else {
      bpm_->UnpinPage(page_->GetPageId(), false);
    }
    bpm_ = nullptr;
    page_ = nullptr;
    is_dirty_ = false;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    // Unpin the current page if it exists
    Drop();

    // Transfer ownership
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;

    // Reset the source guard
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    // Drop the current guard to release resources
    Drop();
    // Transfer ownership
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    // Release the current resources
    Drop();
    // Transfer the state from the source guard
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    // Before using the Drop method of BasicPageGuard, release the write latch
    guard_.page_->WUnlatch();
  }
  // Now, use the BasicPageGuard's Drop to release the other resources
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  // The destructor should behave like the Drop method
  Drop();
}  // NOLINT

}  // namespace bustub
