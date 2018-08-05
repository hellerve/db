describe 'database' do
  DB_FILE = "test"

  def run_script(commands, delete=true)
    raw_output = nil
    IO.popen(["bin/db", DB_FILE], "r+") do |pipe|
      commands.each do |command|
        pipe.puts command
      end

      pipe.close_write

      # Read entire output
      raw_output = pipe.gets(nil)
    end
    File.delete(DB_FILE) if delete
    raw_output.split("\n")
  end

  it 'inserts and retreives a row' do
    result = run_script([
      "insert 1 user1 person1@example.com",
      "select",
      ":q",
    ])
    expect(result).to match_array([
      "(1, user1, person1@example.com)",
      "Goodbye!",
    ])
  end

  it 'prints error message when table is full' do
    script = (1..1401).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ":q"
    result = run_script(script)
    expect(result[-2]).to eq('Error: table full!')
  end


  it 'allows inserting strings that are the maximum length' do
    long_username = "a"*32
    long_email = "a"*255
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ":q",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "(1, #{long_username}, #{long_email})",
      "Goodbye!",
    ])
  end

  it 'prints error message if strings are too long' do
    long_username = "a"*33
    long_email = "a"*256
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ":q",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "A string is too long.",
      "Goodbye!",
    ])
  end

  it 'prints an error message if id is negative' do
    script = [
      "insert -1 cstack foo@bar.com",
      "select",
      ":q",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "ID must be positive.",
      "Goodbye!",
    ])
  end

  it 'keeps data after closing connection' do
    result1 = run_script([
      "insert 1 user1 person1@example.com",
      ":q",
    ], delete=false)
    expect(result1).to match_array([
      "Goodbye!",
    ])
    result2 = run_script([
      "select",
      ":q",
    ])
    expect(result2).to match_array([
      "(1, user1, person1@example.com)",
      "Goodbye!",
    ])
  end

  it 'prints constants' do
      script = [
        ":c",
        ":q",
      ]
      result = run_script(script)

      expect(result).to match_array([
        "Constants:",
        "ROW_SIZE: 296",
        "NODE_HDR_SIZE: 6",
        "LNODE_HEADER_SIZE: 10",
        "LNODE_CELL_SIZE: 300",
        "LNODE_SPACE_FOR_CELLS: 4086",
        "LNODE_MAX_CELLS: 13",
        "Goodbye!",
      ])
  end

  it 'allows printing out the structure of a one-node btree' do
      script = [3, 1, 2].map do |i|
        "insert #{i} user#{i} person#{i}@example.com"
      end
      script << ":tree"
      script << ":q"
      result = run_script(script)

      expect(result).to match_array([
        "Tree:",
        "leaf (size 3)",
        "  - 0 : 1",
        "  - 1 : 2",
        "  - 2 : 3",
        "Goodbye!"
      ])
  end

  it 'allows printing complete debug information' do
      script = [3, 1, 2].map do |i|
        "insert #{i} user#{i} person#{i}@example.com"
      end
      script << ":d"
      script << ":q"
      result = run_script(script)

      expect(result).to match_array([
        "Constants:",
        "ROW_SIZE: 296",
        "NODE_HDR_SIZE: 6",
        "LNODE_HEADER_SIZE: 10",
        "LNODE_CELL_SIZE: 300",
        "LNODE_SPACE_FOR_CELLS: 4086",
        "LNODE_MAX_CELLS: 13",
        "",
        "Tree:",
        "leaf (size 3)",
        "  - 0 : 1",
        "  - 1 : 2",
        "  - 2 : 3",
        "Goodbye!"
      ])
  end

  it 'prints an error message if there is a duplicate id' do
    script = [
      "insert 1 user1 person1@example.com",
      "insert 1 user1 person1@example.com",
      "select",
      ":q",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "Error: duplicate key!",
      "(1, user1, person1@example.com)",
      "Goodbye!",
    ])
  end
end
