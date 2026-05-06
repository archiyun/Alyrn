#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/time/timestamp.h"

#include <cstddef>
#include <vector>

namespace runtime::net {

class Timer;


 
// The red-black tree code is based on the algorithm described in
// the "Introduction to Algorithms" by Cormen, Leiserson and Rivest.
enum class TimerTreeColor : uint8_t {
  kRed = 0,
  kBlack,
};

struct TimerTreeNode {
  Timer* timer_key{nullptr};
  TimerTreeNode* parent{nullptr};
  TimerTreeNode* left{nullptr};
  TimerTreeNode* right{nullptr};
  TimerTreeColor color{TimerTreeColor::kBlack};
  bool in_tree{false};
};

class TimerTree : public runtime::base::NonCopyable {
public:
  TimerTree();

  bool Empty() const { return size_ == 0; }
  std::size_t Size() const { return size_; }
  
  Timer* Earliest() const;
  void Insert(Timer* timer);
  bool Erase(Timer* timer);
  std::vector<Timer*> PopExpired(runtime::time::Timestamp now);
private:
  static TimerTreeNode* Sentinel();
  static inline bool IsRed(const TimerTreeNode* node) { return node->color == TimerTreeColor::kRed; }
  static inline bool IsBlack(const TimerTreeNode* node) { return !IsRed(node); }
  static inline void Red(TimerTreeNode* node) { node->color = TimerTreeColor::kRed; }
  static inline void Black(TimerTreeNode* node) { node->color = TimerTreeColor::kBlack; }
  static inline void CopyColor(TimerTreeNode* dst, TimerTreeNode* src) { dst->color = src->color; }
  static bool Less(const Timer* lhs, const Timer* rhs);
  static TimerTreeNode* Minimum(TimerTreeNode* node);

  static TimerTreeNode* NodeOf(Timer* timer) ;

  void LeftRotate(TimerTreeNode* node);
  void RightRotate(TimerTreeNode* node);

  void InsertFixup(TimerTreeNode* node);
  void DeleteFixup(TimerTreeNode* node);

  TimerTreeNode* Next(TimerTreeNode* node);
  void Transplant(TimerTreeNode* old_node, TimerTreeNode* new_node);
  void ResetNode(TimerTreeNode* node);
private:
  TimerTreeNode* sentinel_;
  TimerTreeNode* root_;
  std::size_t size_{0};
};

} // namespace runtime::net

