import os
import sys
from github import Github
from openai import OpenAI

# 1. 初始化配置
SOURCE_REPO_NAME = os.getenv("GITHUB_REPOSITORY") # 当前代码库
TARGET_DOC_REPO_NAME = "risingwavelabs/risingwave-docs" # 目标文档库 (请根据实际情况修改)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GITHUB_TOKEN = os.getenv("DOC_REPO_PAT") # 使用 PAT 以便跨仓库操作
PR_NUMBER = int(os.getenv("PR_NUMBER"))

# 验证环境
if not all([OPENAI_API_KEY, GITHUB_TOKEN, PR_NUMBER]):
    print("::error::Missing required environment variables.")
    sys.exit(1)

client = OpenAI(api_key=OPENAI_API_KEY)
gh = Github(GITHUB_TOKEN)

def get_pr_diff():
    """获取原代码仓库 PR 的代码变更"""
    repo = gh.get_repo(SOURCE_REPO_NAME)
    pr = repo.get_pull(PR_NUMBER)
    
    # 获取 diff (简单起见，这里获取所有文件变更的 patch)
    # 实际生产中可能需要过滤掉非 .go/.rs/.java 等核心代码文件
    diff_text = ""
    for file in pr.get_files():
        if file.patch:
            diff_text += f"File: {file.filename}\n{file.patch}\n\n"
    
    return pr, diff_text[:15000] # 截断以防止 Token 溢出

def generate_doc_content(pr, diff):
    """调用 AI 生成文档"""
    prompt = f"""
    你是一个技术文档编写专家。请根据以下 GitHub PR 的信息和代码变更 (Diff)，
    编写一份 Markdown 格式的功能更新文档。
    
    PR 标题: {pr.title}
    PR 描述: {pr.body}
    
    代码变更:
    {diff}
    
    要求:
    1. 这是一个 {TARGET_DOC_REPO_NAME} 的文档更新。
    2. 如果是新功能，解释其用途和用法。
    3. 如果是配置项变更，列出新的参数名。
    4. 输出内容不包含 ```markdown 这样的包裹符号，直接输出正文。
    """
    
    print("::group::Sending request to OpenAI...")
    response = client.chat.completions.create(
        model="gpt-4o", # 或 gpt-3.5-turbo
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    print("::endgroup::")
    return response.choices[0].message.content

def create_doc_pr(source_pr, content):
    """在文档仓库创建新的分支和 PR"""
    doc_repo = gh.get_repo(TARGET_DOC_REPO_NAME)
    base_branch = "main" # 文档库的主分支名，可能是 master
    
    # 1. 准备新分支名
    new_branch_name = f"ai-docs/pr-{source_pr.number}"
    
    # 2. 获取 base 分支的 sha
    try:
        sb = doc_repo.get_branch(base_branch)
        doc_repo.create_git_ref(ref=f"refs/heads/{new_branch_name}", sha=sb.commit.sha)
    except Exception as e:
        print(f"::warning::Branch creation failed (might exist): {e}")
        # 如果分支存在，尝试继续（可能会覆盖）

    # 3. 创建文件 (在这个 Demo 中，我们创建一个新的 markdown 文件)
    # 实际场景中，AI 甚至可以指定修改哪个已存在的文件，但这需要更复杂的 Logic
    file_path = f"ai-generated/pr-{source_pr.number}.md"
    message = f"docs: auto-generated from {SOURCE_REPO_NAME}#{source_pr.number}"
    
    try:
        # 检查文件是否存在以决定是 create 还是 update
        contents = doc_repo.get_contents(file_path, ref=new_branch_name)
        doc_repo.update_file(file_path, message, content, contents.sha, branch=new_branch_name)
    except:
        doc_repo.create_file(file_path, message, content, branch=new_branch_name)

    # 4. 提 PR
    try:
        new_pr = doc_repo.create_pull(
            title=f"🤖 AI Docs: {source_pr.title}",
            body=f"Generated based on code PR: {source_pr.html_url}\n\nPlease review the content.",
            head=new_branch_name,
            base=base_branch
        )
        print(f"::notice::Successfully created Doc PR: {new_pr.html_url}")
    except Exception as e:
        print(f"::warning::PR creation failed (might exist): {e}")

if __name__ == "__main__":
    print("Starting AI Doc Agent...")
    pr, diff = get_pr_diff()
    if not diff:
        print("No code changes found. Exiting.")
        sys.exit(0)
        
    doc_content = generate_doc_content(pr, diff)
    create_doc_pr(pr, doc_content)
    print("Done.")