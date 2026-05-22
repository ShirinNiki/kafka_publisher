const form = document.getElementById('form');
const topicEl = document.getElementById('topic');
const messagesEl = document.getElementById('messages');
const fileInput = document.getElementById('file');
const pickFileBtn = document.getElementById('pick-file');
const fileNameEl = document.getElementById('file-name');
const resultEl = document.getElementById('result');
const submitBtn = document.getElementById('submit');

function showResult(text, ok) {
  resultEl.hidden = false;
  resultEl.textContent = text;
  resultEl.classList.remove('ok', 'err');
  resultEl.classList.add(ok ? 'ok' : 'err');
}

pickFileBtn.addEventListener('click', () => fileInput.click());

fileInput.addEventListener('change', () => {
  const file = fileInput.files?.[0];
  fileNameEl.textContent = '';
  if (!file) {
    return;
  }

  const reader = new FileReader();
  reader.onload = () => {
    try {
      const parsed = JSON.parse(/** @type {string} */ (reader.result));
      if (!Array.isArray(parsed)) {
        showResult('JSON file must contain a JSON array (e.g. [ { … }, … ]).', false);
        fileInput.value = '';
        return;
      }
      messagesEl.value = JSON.stringify(parsed, null, 2);
      fileNameEl.textContent = file.name;
      resultEl.hidden = true;
    } catch (err) {
      showResult(`Invalid JSON in file: ${err.message}`, false);
      fileInput.value = '';
    }
  };
  reader.onerror = () => {
    showResult('Could not read the file.', false);
    fileInput.value = '';
  };
  reader.readAsText(file, 'UTF-8');
});

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  resultEl.hidden = true;
  submitBtn.disabled = true;

  let raw = messagesEl.value.trim();
  if (!raw && fileInput.files?.[0]) {
    try {
      raw = await fileInput.files[0].text();
    } catch {
      showResult('Could not read the selected file.', false);
      submitBtn.disabled = false;
      return;
    }
  }

  if (!raw) {
    showResult('Add a JSON array in the text area or choose a .json file.', false);
    submitBtn.disabled = false;
    return;
  }

  let messages;
  try {
    messages = JSON.parse(raw);
  } catch (err) {
    showResult(`Invalid JSON: ${err.message}`, false);
    submitBtn.disabled = false;
    return;
  }

  if (!Array.isArray(messages)) {
    showResult('Messages must be a JSON array.', false);
    submitBtn.disabled = false;
    return;
  }

  if (messages.length === 0) {
    showResult('The array must contain at least one message.', false);
    submitBtn.disabled = false;
    return;
  }

  const body = {
    topic: topicEl.value.trim(),
    messages,
  };

  try {
    const res = await fetch('/api/publish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json().catch(() => ({}));

    if (!res.ok) {
      const detail =
        data.errors?.length > 0
          ? `${data.error || 'Error'}\n${JSON.stringify(data.errors, null, 2)}`
          : data.error || res.statusText;
      showResult(`${res.status} ${detail}`, false);
      return;
    }

    showResult(JSON.stringify(data, null, 2), true);
  } catch (err) {
    showResult(String(err.message || err), false);
  } finally {
    submitBtn.disabled = false;
  }
});
