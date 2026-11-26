import { fitGBT, fitOLS, metrics, modelData } from '../sim/algorithms.js'

// Model simülasyonu: örneklemde OLS + GBT eğitir, ağaç ağaç test RMSE eğrisini döndürür.
self.onmessage = ({ data }) => {
  const { rows, size, nTrees, maxDepth, learningRate, includeLeaky } = data
  const t0 = performance.now()
  const md = modelData(rows, { size, includeLeaky })
  const ols = fitOLS(md.Xtr, md.ytr)
  const olsM = metrics(md.yte, md.Xte.map(ols.predict))
  const curve = []
  const testPred = new Float64Array(md.yte.length)
  const model = fitGBT(md.Xtr, md.ytr, { nTrees, maxDepth, learningRate, minLeaf: 20 })
  testPred.fill(model.init)
  model.trees.forEach((_, t) => {
    // aşamalı tahmin: ilk t+1 ağaç
    for (let i = 0; i < md.Xte.length; i += 1) testPred[i] += learningRate * evalTree(model.trees[t], md.Xte[i])
    curve.push(metrics(md.yte, testPred).rmse)
  })
  const final = metrics(md.yte, Array.from(testPred))
  self.postMessage({ ols: olsM, gbt: final, curve, nTrain: md.ytr.length, nTest: md.yte.length, feats: md.feats, ms: performance.now() - t0 })
}

function evalTree(node, x) {
  let cur = node
  while (cur.feature !== undefined) cur = x[cur.feature] <= cur.threshold ? cur.left : cur.right
  return cur.value
}
