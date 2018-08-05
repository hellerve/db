describe 'database' do
  DB_FILE = "test"

  def run_script(commands, delete=true)
    raw_output = nil
    IO.popen(["bin/db", DB_FILE], "r+") do |pipe|
      commands.each do |command|
        begin
          pipe.puts command
        rescue Errno::EPIPIE
          break
        end
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
        "LNODE_HDR_SIZE: 14",
        "LNODE_CELL_SIZE: 300",
        "LNODE_SPACE_FOR_CELLS: 4082",
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
        "- leaf (size 3)",
        "  - 1",
        "  - 2",
        "  - 3",
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
        "LNODE_HDR_SIZE: 14",
        "LNODE_CELL_SIZE: 300",
        "LNODE_SPACE_FOR_CELLS: 4082",
        "LNODE_MAX_CELLS: 13",
        "",
        "Tree:",
        "- leaf (size 3)",
        "  - 1",
        "  - 2",
        "  - 3",
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

  it 'allows printing out the structure of a 3-leaf-node btree' do
    script = (1..14).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ":tree"
    script << ":q"
    result = run_script(script)
    print(result)

    expect(result[14...(result.length)]).to match_array([
      "Tree:",
      "- internal (size 1)",
      "  - leaf (size 7)",
      "    - 1",
      "    - 2",
      "    - 3",
      "    - 4",
      "    - 5",
      "    - 6",
      "    - 7",
      "- key 7",
      "  - leaf (size 7)",
      "    - 8",
      "    - 9",
      "    - 10",
      "    - 11",
      "    - 12",
      "    - 13",
      "    - 14",
      "Goodbye!"
    ])
  end

  it 'allows printing out the structure of a 4-leaf-node btree' do
    script = [
      "insert 18 user18 person18@example.com",
      "insert 7 user7 person7@example.com",
      "insert 10 user10 person10@example.com",
      "insert 29 user29 person29@example.com",
      "insert 23 user23 person23@example.com",
      "insert 4 user4 person4@example.com",
      "insert 14 user14 person14@example.com",
      "insert 30 user30 person30@example.com",
      "insert 15 user15 person15@example.com",
      "insert 26 user26 person26@example.com",
      "insert 22 user22 person22@example.com",
      "insert 19 user19 person19@example.com",
      "insert 2 user2 person2@example.com",
      "insert 1 user1 person1@example.com",
      "insert 21 user21 person21@example.com",
      "insert 11 user11 person11@example.com",
      "insert 6 user6 person6@example.com",
      "insert 20 user20 person20@example.com",
      "insert 5 user5 person5@example.com",
      "insert 8 user8 person8@example.com",
      "insert 9 user9 person9@example.com",
      "insert 3 user3 person3@example.com",
      "insert 12 user12 person12@example.com",
      "insert 27 user27 person27@example.com",
      "insert 17 user17 person17@example.com",
      "insert 16 user16 person16@example.com",
      "insert 13 user13 person13@example.com",
      "insert 24 user24 person24@example.com",
      "insert 25 user25 person25@example.com",
      "insert 28 user28 person28@example.com",
      ":tree",
      ":q",
    ]
    result = run_script(script)
    print(result)
  end
end
