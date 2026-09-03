const nodes = Object.fromEntries(
  [...document.querySelectorAll("[data-node]")].map((element) => [element.dataset.node, element]),
);

const events = document.querySelector("#events");
const status = document.querySelector("#run-status");
const runButton = document.querySelector("#run-button");
const condition = document.querySelector("#condition");
const flaky = document.querySelector("#flaky");

const wait = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

function setNode(name, state, detail) {
  const node = nodes[name];
  node.classList.remove("running", "succeeded", "failed", "skipped");
  if (state) node.classList.add(state);
  node.querySelector("small").textContent = detail;
}

function emit(type, detail = "") {
  const item = document.createElement("li");
  item.textContent = detail ? `${type} · ${detail}` : type;
  events.append(item);
}

function reset() {
  events.replaceChildren();
  status.textContent = "running";
  for (const name of Object.keys(nodes)) setNode(name, "", "pending");
}

async function executeNode(name, attempt = 1) {
  setNode(name, "running", `attempt ${attempt}`);
  emit("node.started", `${name} · attempt ${attempt}`);
  await wait(220);
}

async function succeedNode(name, attempt = 1) {
  setNode(name, "succeeded", `succeeded · ${attempt} attempt${attempt === 1 ? "" : "s"}`);
  emit("node.succeeded", `${name} · attempt ${attempt}`);
  await wait(120);
}

async function run() {
  runButton.disabled = true;
  reset();
  emit("run.started", "demo-run");

  await executeNode("start");
  await succeedNode("start");

  await executeNode("if");
  await succeedNode("if");

  const activeBranch = condition.value === "true" ? "yes" : "no";
  const inactiveBranch = activeBranch === "yes" ? "no" : "yes";
  setNode(inactiveBranch, "skipped", "inactive inputs");
  emit("node.skipped", `${inactiveBranch} · inactive-inputs`);
  await executeNode(activeBranch);
  await succeedNode(activeBranch);

  await executeNode("flaky");

  if (flaky.value === "fail") {
    emit("node.retrying", "work · attempt 1");
    setNode("flaky", "running", "retrying");
    await wait(220);
    emit("node.started", "work · attempt 2");
    await wait(220);
    setNode("flaky", "failed", "failed · 2 attempts");
    emit("node.failed", "work · attempt 2");
    status.textContent = "failed";
    emit("run.failed", "node-failed");
    setNode("end", "skipped", "run failed");
    runButton.disabled = false;
    return;
  }

  if (flaky.value === "retry") {
    emit("node.retrying", "work · attempt 1");
    setNode("flaky", "running", "retrying");
    await wait(220);
    emit("node.started", "work · attempt 2");
    await wait(220);
    await succeedNode("flaky", 2);
  } else {
    await succeedNode("flaky");
  }

  await executeNode("end");
  await succeedNode("end");
  status.textContent = "succeeded";
  emit("run.succeeded", `active branch: ${activeBranch}`);
  runButton.disabled = false;
}

runButton.addEventListener("click", run);
