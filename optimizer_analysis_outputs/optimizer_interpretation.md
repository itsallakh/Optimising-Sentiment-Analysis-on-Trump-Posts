# Optimizer Tradeoff Interpretation

This analysis keeps the modeling setup fixed: same dataset, same TF-IDF features, same binary logistic regression model, same L2 baseline, and same train/test split. The only moving parts are optimizer-related settings.

## Mathematical Optimization Problem

The experiments train binary logistic regression with L2 regularization on TF-IDF features. Let:

- $X \in \mathbb{R}^{n \times d}$ be the TF-IDF feature matrix, with row $x_i$ representing one post.
- $y \in \{0,1\}^n$ be the binary labels, where 1 is positive sentiment and 0 is negative sentiment.
- $w \in \mathbb{R}^d$ be the model parameter vector.
- $b \in \mathbb{R}$ be the bias term.
- $\lambda \ge 0$ be the L2 regularization strength.

The prediction for example $i$ is:

$$
p_i = \sigma(w^T x_i + b) = \frac{1}{1 + e^{-(w^T x_i + b)}}
$$

The objective minimized by the optimizers is binary cross-entropy with L2 regularization:

$$
J(w,b) =
-\frac{1}{n}\sum_{i=1}^{n}
\left[
y_i \log(p_i) + (1-y_i)\log(1-p_i)
\right]
+ \frac{\lambda}{2}\lVert w\rVert_2^2
$$

The full-training-set gradients are:

$$
\nabla_w J =
\frac{1}{n}X^T(p-y) + \lambda w
$$

$$
\nabla_b J =
\frac{1}{n}\sum_{i=1}^{n}(p_i-y_i)
$$

Batch Gradient Descent uses these gradients over the full training set:

$$
w \leftarrow w - \eta \nabla_w J,
\quad
b \leftarrow b - \eta \nabla_b J
$$

Stochastic Gradient Descent updates after each single example $(x_i,y_i)$:

$$
\nabla_w J_i = (p_i-y_i)x_i + \lambda w,
\quad
\nabla_b J_i = p_i-y_i
$$

$$
w \leftarrow w - \eta \nabla_w J_i,
\quad
b \leftarrow b - \eta \nabla_b J_i
$$

Mini-batch Gradient Descent updates using a mini-batch $B$:

$$
\nabla_w J_B =
\frac{1}{|B|}\sum_{i \in B}(p_i-y_i)x_i + \lambda w,
\quad
\nabla_b J_B =
\frac{1}{|B|}\sum_{i \in B}(p_i-y_i)
$$

$$
w \leftarrow w - \eta \nabla_w J_B,
\quad
b \leftarrow b - \eta \nabla_b J_B
$$

The three methods differ mainly in how often they update parameters and how much data each update uses. Batch GD updates least often and has the smoothest gradient estimate. SGD updates most often and is useful for sparse TF-IDF examples, but its convergence path is noisier and more seed-sensitive. Mini-batch GD uses sparse TF-IDF efficiently while averaging over enough examples to reduce noise, which explains why it is a strong practical compromise in the saved results.

## Main Takeaways

- Fastest convergence by the saved 90% loss-reduction marker: Mini-batch GD (lr=1, batch=64, L2=0.01), reaching that point at epoch 2.
- Smoothest loss trajectory: Batch GD (lr=4, L2=0.0001), with loss roughness 0.000962.
- Most fluctuating loss trajectory: Batch GD (lr=12, L2=0.0001), with loss roughness 0.031256.
- Best final test macro F1: SGD (lr=0.05, L2=1e-05), macro F1 0.8410, runtime 15.926 seconds.
- Fastest run: Batch GD (lr=1, L2=0.0001), runtime 0.071 seconds, macro F1 0.4493.
- Practical runtime/performance winner: Mini-batch GD (lr=2, batch=64, L2=0.0001), macro F1 0.8087, runtime 0.617 seconds.

## Convergence, Noise, and Speed

The fastest convergence marker belongs to Mini-batch GD. The smoothest observed curve belongs to Batch GD, while the roughest belongs to Batch GD. This matches the expected optimization tradeoff: methods that update more frequently can reach useful regions quickly, but their loss paths can be noisier than full-batch updates.

Runtime tells a different part of the story than final macro F1. Batch GD has low per-run overhead in these saved runs, SGD explores aggressively but can be much slower, and Mini-batch GD often sits in the middle: enough stochasticity to improve learning behavior, but not so much update-by-update cost that runtime dominates the result.

## Why Mini-batch GD Is a Strong Practical Choice

SGD can achieve the top final macro F1 in the saved results: 0.8410 at 15.926 seconds. Mini-batch GD's strongest practical run reaches macro F1 0.8087 at 0.617 seconds.
That makes Mini-batch GD easy to defend for a final report: the goal is not only the single highest endpoint, but the balance among final classification performance, convergence behavior, runtime, and stability. A mini-batch update uses more signal per step than SGD while avoiding the fully deterministic, sometimes slower-to-improve behavior of Batch GD.

## Learning Rate Sensitivity

In the learning-rate scenario, the best macro F1 comes from SGD (lr=0.1, L2=0.0001) with macro F1 0.8227. The best practical tradeoff in that same scenario is Mini-batch GD (lr=2, batch=64, L2=0.0001).
The saved curves show that learning rate changes both the endpoint and the shape of optimization. Smaller rates tend to move conservatively; larger rates can reduce loss faster, but they can also increase roughness or push the optimizer into less useful behavior if the step size is too aggressive.

## Mini-batch Size Tradeoff

Within the mini-batch-size scenario, the best macro F1 is batch size 8, with macro F1 0.8227. The fastest mini-batch run is batch size 128, with runtime 0.385 seconds.
The batch-size results show the practical compromise directly: smaller batches make more frequent noisy updates, while larger batches use more examples per update and can be smoother, but may not give the best runtime/performance point.

## Seed Stability

Across seeds, SGD has the higher mean macro F1 (0.7940 +/- 0.0192). Mini-batch GD has the lower macro-F1 standard deviation (0.0180), and Mini-batch GD is faster on average (1.014 seconds).
The seed-stability result means the optimizer choice affects reproducibility, not just average performance. A small standard deviation suggests that the optimizer is less dependent on random initialization or data order for this fixed model and feature setup.

## L2 Interaction

In the L2-interaction scenario, the best macro F1 is produced by SGD (lr=0.05, L2=1e-05), macro F1 0.8410.
Changing L2 strength changes the optimization landscape and the amount of shrinkage on the logistic-regression weights. In the saved results, the best regularization strength is therefore optimizer-dependent: the same optimizer settings do not behave identically as the penalty changes.

## Animation

The saved loss history includes epoch-level training loss, so a loss-curve animation was created.

## Final Conclusion

Plain-language takeaway: for this fixed logistic-regression sentiment setup, Mini-batch GD is the strongest practical optimizer choice because it gives the best saved balance of classification quality, runtime, and loss behavior, while SGD remains valuable when the only target is the highest final macro F1.
