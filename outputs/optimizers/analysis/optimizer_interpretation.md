# Optimizer Tradeoff Interpretation

This analysis keeps the modeling setup fixed: same dataset, same TF-IDF features, same binary logistic regression model, same L2 baseline, and same train/test split. The only moving parts are optimizer-related settings.

## Main Takeaways

- Fastest convergence by the saved 90% loss-reduction marker: Mini-batch GD (lr=1, batch=64, L2=0.01), reaching that point at epoch 2.
- Smoothest loss trajectory: Batch GD (lr=4, L2=0.0001), with loss roughness 0.000976.
- Most fluctuating loss trajectory: Batch GD (lr=12, L2=0.0001), with loss roughness 0.031305.
- Best final test macro F1: SGD (lr=0.05, L2=1e-05), macro F1 0.8149, runtime 15.728 seconds.
- Fastest run: Batch GD (lr=8, L2=0.0001), runtime 0.077 seconds, macro F1 0.5041.
- Practical runtime/performance winner: Mini-batch GD (lr=2, batch=64, L2=0.0001), macro F1 0.7557, runtime 0.717 seconds.

## Convergence, Noise, and Speed

The fastest convergence marker belongs to Mini-batch GD. The smoothest observed curve belongs to Batch GD, while the roughest belongs to Batch GD. This matches the expected optimization tradeoff: methods that update more frequently can reach useful regions quickly, but their loss paths can be noisier than full-batch updates.

Runtime tells a different part of the story than final macro F1. Batch GD has low per-run overhead in these saved runs, SGD explores aggressively but can be much slower, and Mini-batch GD often sits in the middle: enough stochasticity to improve learning behavior, but not so much update-by-update cost that runtime dominates the result.

## Why Mini-batch GD Is a Strong Practical Choice

SGD can achieve the top final macro F1 in the saved results: 0.8149 at 15.728 seconds. Mini-batch GD's strongest practical run reaches macro F1 0.7557 at 0.717 seconds.
That makes Mini-batch GD easy to defend for a final report: the goal is not only the single highest endpoint, but the balance among final classification performance, convergence behavior, runtime, and stability. A mini-batch update uses more signal per step than SGD while avoiding the fully deterministic, sometimes slower-to-improve behavior of Batch GD.

## Learning Rate Sensitivity

In the learning-rate scenario, the best macro F1 comes from SGD (lr=0.1, L2=0.0001) with macro F1 0.7726. The best practical tradeoff in that same scenario is Mini-batch GD (lr=2, batch=64, L2=0.0001).
The saved curves show that learning rate changes both the endpoint and the shape of optimization. Smaller rates tend to move conservatively; larger rates can reduce loss faster, but they can also increase roughness or push the optimizer into less useful behavior if the step size is too aggressive.

## Mini-batch Size Tradeoff

Within the mini-batch-size scenario, the best macro F1 is batch size 8, with macro F1 0.7770. The fastest mini-batch run is batch size 128, with runtime 0.434 seconds.
The batch-size results show the practical compromise directly: smaller batches make more frequent noisy updates, while larger batches use more examples per update and can be smoother, but may not give the best runtime/performance point.

## Seed Stability

Across seeds, SGD has the higher mean macro F1 (0.7659 +/- 0.0180). Mini-batch GD has the lower macro-F1 standard deviation (0.0046), and Mini-batch GD is faster on average (0.727 seconds).
The seed-stability result means the optimizer choice affects reproducibility, not just average performance. A small standard deviation suggests that the optimizer is less dependent on random initialization or data order for this fixed model and feature setup.

## L2 Interaction

In the L2-interaction scenario, the best macro F1 is produced by SGD (lr=0.05, L2=1e-05), macro F1 0.8149.
Changing L2 strength changes the optimization landscape and the amount of shrinkage on the logistic-regression weights. In the saved results, the best regularization strength is therefore optimizer-dependent: the same optimizer settings do not behave identically as the penalty changes.

## Animation

The saved loss history includes epoch-level training loss, so a loss-curve animation was created.

## Final Conclusion

Plain-language takeaway: for this fixed logistic-regression sentiment setup, Mini-batch GD is the strongest practical optimizer choice because it gives the best saved balance of classification quality, runtime, and loss behavior, while SGD remains valuable when the only target is the highest final macro F1.
