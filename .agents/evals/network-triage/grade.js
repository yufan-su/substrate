// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Grades the report's Category, Component, and Root cause lines against the
// case's vars. root_cause may be a list of acceptable answers.
module.exports = (output, context) => {
  const field = (name) => {
    const match = output.match(new RegExp(`^[\\s>*_-]*${name}[*_]*:[*_]*\\s*(.+)$`, 'mi'));
    return match ? match[1].replace(/[*`]/g, '').trim().toLowerCase() : '';
  };
  const { category, component, root_cause: rootCause } = context.vars;
  const failures = [];

  const gotCategory = field('Category');
  if (!gotCategory.startsWith(category.toLowerCase())) {
    failures.push(`Category: got "${gotCategory}", want "${category}"`);
  }
  const gotComponent = field('Component');
  if (!gotComponent.startsWith(component.toLowerCase())) {
    failures.push(`Component: got "${gotComponent}", want "${component}"`);
  }
  const gotRootCause = field('Root cause');
  const wantRootCause = [].concat(rootCause).map((s) => s.toLowerCase());
  if (!wantRootCause.some((s) => gotRootCause.includes(s))) {
    failures.push(`Root cause: got "${gotRootCause}", want one of ${JSON.stringify(wantRootCause)}`);
  }

  return {
    pass: failures.length === 0,
    score: 1 - failures.length / 3,
    reason: failures.join('; ') || 'Category, Component, and Root cause match',
  };
};
